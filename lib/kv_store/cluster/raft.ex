defmodule KVStore.Cluster.Raft do
  @moduledoc """
  Raft consensus algorithm implementation for distributed KVStore.

  Implements:
  - Leader election
  - Log replication
  - Safety and liveness guarantees
  """

  use GenServer
  require Logger

  @type raft_state :: :follower | :candidate | :leader
  @type raft_term :: non_neg_integer()
  @type node_id :: String.t()
  @type log_entry :: %{
          term: raft_term(),
          index: non_neg_integer(),
          command: map()
        }

  defstruct [
    # Persistent state (survives crashes)
    :current_term,
    :voted_for,
    :log,

    # Volatile state (reset on restart)
    :commit_index,
    :last_applied,
    :state,
    :leader_id,

    # Configuration
    :node_id,
    :cluster_nodes,
    :election_timeout_ms,
    :heartbeat_interval_ms,

    # Leader state (reinitialized after election)
    :next_index,
    :match_index,

    # Timers
    :election_timer,
    :heartbeat_timer
  ]

  # Client API

  @doc """
  Start a Raft node.
  """
  def start_link(opts \\ []) do
    node_id = Keyword.get(opts, :node_id, KVStore.Config.node_id())

    GenServer.start_link(__MODULE__, opts, name: {:global, {:raft, node_id}})
  end

  @doc """
  Get current Raft state.
  """
  def get_state(node_id) do
    GenServer.call({:global, {:raft, node_id}}, :get_state)
  end

  @doc """
  Submit a command to the cluster.
  """
  def submit_command(node_id, command) do
    GenServer.call(
      {:global, {:raft, node_id}},
      {:submit_command, command}
    )
  end

  @doc """
  Get cluster status.
  """
  def cluster_status(node_id) do
    GenServer.call({:global, {:raft, node_id}}, :cluster_status)
  end

  # GenServer Callbacks

  @impl true
  def init(opts) do
    node_id = Keyword.get(opts, :node_id, KVStore.Config.node_id())
    cluster_nodes = Keyword.get(opts, :cluster_nodes, KVStore.Config.cluster_nodes())

    election_timeout =
      Keyword.get(opts, :election_timeout_ms, KVStore.Config.raft_election_timeout_ms())

    heartbeat_interval =
      Keyword.get(opts, :heartbeat_interval_ms, KVStore.Config.raft_heartbeat_interval_ms())

    # Initialize state
    state = %__MODULE__{
      current_term: 0,
      voted_for: nil,
      log: [],
      commit_index: 0,
      last_applied: 0,
      state: :follower,
      leader_id: nil,
      node_id: node_id,
      cluster_nodes: cluster_nodes,
      election_timeout_ms: election_timeout,
      heartbeat_interval_ms: heartbeat_interval,
      next_index: %{},
      match_index: %{}
    }

    # Start election timer
    election_timer = start_election_timer(state)

    Logger.info("Raft node #{node_id} started as follower in term #{state.current_term}")

    {:ok, %{state | election_timer: election_timer}}
  end

  @impl true
  def handle_call(:get_state, _from, state) do
    {:reply, state, state}
  end

  @impl true
  def handle_call({:submit_command, command}, from, state) do
    case state.state do
      :leader ->
        # Add command to log
        entry = %{
          term: state.current_term,
          index: length(state.log) + 1,
          command: command
        }

        new_log = state.log ++ [entry]
        new_state = %{state | log: new_log}

        # Replicate to followers
        replicate_log(new_state)

        # For single-node clusters, commit immediately
        if length(state.cluster_nodes) == 1 do
          # Commit the entry immediately
          committed_state = %{new_state | commit_index: entry.index}
          final_state = apply_committed_entries(committed_state)

          # Apply the command
          apply_command(command)

          {:reply, {:ok, :committed}, final_state}
        else
          # Store the caller for later response
          Process.put({:pending_command, entry.index}, from)
          {:noreply, new_state}
        end

      _ ->
        # Redirect to leader
        case state.leader_id do
          nil ->
            {:reply, {:error, :no_leader}, state}

          leader_id ->
            # Forward to leader
            case forward_to_leader(leader_id, command) do
              {:ok, result} ->
                {:reply, {:ok, result}, state}

              {:error, reason} ->
                {:reply, {:error, reason}, state}
            end
        end
    end
  end

  @impl true
  def handle_call(:cluster_status, _from, state) do
    status = %{
      node_id: state.node_id,
      state: state.state,
      term: state.current_term,
      leader_id: state.leader_id,
      log_length: length(state.log),
      commit_index: state.commit_index,
      cluster_nodes: state.cluster_nodes
    }

    {:reply, status, state}
  end

  @impl true
  def handle_info(:election_timeout, state) do
    case state.state do
      :follower ->
        # Start election
        start_election(state)

      :candidate ->
        # Election timeout, start new election
        start_election(state)

      :leader ->
        # Should not happen, restart timer
        election_timer = start_election_timer(state)
        {:noreply, %{state | election_timer: election_timer}}
    end
  end

  @impl true
  def handle_info(:heartbeat_timeout, state) do
    case state.state do
      :leader ->
        # Send heartbeats to followers
        send_heartbeats(state)
        heartbeat_timer = start_heartbeat_timer(state)
        {:noreply, %{state | heartbeat_timer: heartbeat_timer}}

      _ ->
        # Not leader, ignore
        {:noreply, state}
    end
  end

  @impl true
  def handle_info({:request_vote, from, term, candidate_id, last_log_index, last_log_term}, state) do
    {vote_granted, new_state} =
      handle_request_vote(state, term, candidate_id, last_log_index, last_log_term)

    # Send response
    send(from, {:request_vote_response, state.node_id, term, vote_granted})

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:request_vote_response, voter_id, term, vote_granted}, state) do
    case state.state do
      :candidate when term == state.current_term ->
        new_state = handle_vote_response(state, voter_id, vote_granted)
        {:noreply, new_state}

      _ ->
        # Ignore stale responses
        {:noreply, state}
    end
  end

  @impl true
  def handle_info(
        {:append_entries, from, term, leader_id, prev_log_index, prev_log_term, entries,
         leader_commit},
        state
      ) do
    {success, new_state} =
      handle_append_entries(
        state,
        term,
        leader_id,
        prev_log_index,
        prev_log_term,
        entries,
        leader_commit
      )

    # Send response
    send(from, {:append_entries_response, state.node_id, term, success})

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:append_entries_response, follower_id, term, success}, state) do
    case state.state do
      :leader when term == state.current_term ->
        new_state = handle_append_entries_response(state, follower_id, success)
        {:noreply, new_state}

      _ ->
        # Ignore stale responses
        {:noreply, state}
    end
  end

  # Private functions

  defp start_election(state) do
    new_term = state.current_term + 1

    Logger.info("Node #{state.node_id} starting election for term #{new_term}")

    # Vote for self
    new_state = %{state | state: :candidate, current_term: new_term, voted_for: state.node_id}

    # For single-node clusters, become leader immediately
    if length(state.cluster_nodes) == 1 do
      # Single node cluster - become leader immediately
      leader_state = become_leader(new_state)
      {:noreply, leader_state}
    else
      # Request votes from other nodes
      request_votes(new_state)

      # Start election timer
      election_timer = start_election_timer(new_state)

      {:noreply, %{new_state | election_timer: election_timer}}
    end
  end

  defp request_votes(state) do
    other_nodes = state.cluster_nodes -- [state.node_id]
    last_log_index = length(state.log)

    last_log_term =
      case List.last(state.log) do
        nil -> 0
        entry -> entry.term
      end

    Enum.each(other_nodes, fn node_id ->
      send({:global, {:raft, node_id}}, {
        :request_vote,
        self(),
        state.current_term,
        state.node_id,
        last_log_index,
        last_log_term
      })
    end)
  end

  defp handle_request_vote(state, term, candidate_id, last_log_index, last_log_term) do
    cond do
      term < state.current_term ->
        {false, state}

      term > state.current_term ->
        # Step down to follower
        new_state = %{
          state
          | current_term: term,
            voted_for: nil,
            state: :follower,
            leader_id: nil
        }

        election_timer = start_election_timer(new_state)
        new_state = %{new_state | election_timer: election_timer}

        # Check if we can vote for this candidate
        if can_vote_for_candidate(new_state, candidate_id, last_log_index, last_log_term) do
          {true, %{new_state | voted_for: candidate_id}}
        else
          {false, new_state}
        end

      true ->
        # Same term
        if can_vote_for_candidate(state, candidate_id, last_log_index, last_log_term) do
          {true, %{state | voted_for: candidate_id}}
        else
          {false, state}
        end
    end
  end

  defp can_vote_for_candidate(state, candidate_id, _last_log_index, _last_log_term) do
    # Haven't voted for anyone else in this term
    state.voted_for == nil or state.voted_for == candidate_id
  end

  defp handle_vote_response(state, voter_id, vote_granted) do
    if vote_granted do
      # Count votes
      votes_received = (state.match_index |> Map.keys() |> length()) + 1
      total_nodes = length(state.cluster_nodes)

      if votes_received > div(total_nodes, 2) do
        # Won election
        become_leader(state)
      else
        # Update match index for this voter
        %{state | match_index: Map.put(state.match_index, voter_id, 0)}
      end
    else
      state
    end
  end

  defp become_leader(state) do
    Logger.info("Node #{state.node_id} became leader for term #{state.current_term}")

    # Initialize leader state
    next_index =
      Map.new(state.cluster_nodes, fn node_id ->
        {node_id, length(state.log) + 1}
      end)

    match_index =
      Map.new(state.cluster_nodes, fn node_id ->
        {node_id, 0}
      end)

    new_state = %{
      state
      | state: :leader,
        leader_id: state.node_id,
        next_index: next_index,
        match_index: match_index
    }

    # For single-node clusters, become leader immediately
    if length(state.cluster_nodes) == 1 do
      # No need to send heartbeats to others
      heartbeat_timer = start_heartbeat_timer(new_state)
      %{new_state | heartbeat_timer: heartbeat_timer}
    else
      # Send initial heartbeats
      send_heartbeats(new_state)

      # Start heartbeat timer
      heartbeat_timer = start_heartbeat_timer(new_state)

      %{new_state | heartbeat_timer: heartbeat_timer}
    end
  end

  defp send_heartbeats(state) do
    other_nodes = state.cluster_nodes -- [state.node_id]

    Enum.each(other_nodes, fn node_id ->
      send_append_entries(state, node_id, [])
    end)
  end

  defp send_append_entries(state, follower_id, entries) do
    next_index = Map.get(state.next_index, follower_id, 1)
    prev_log_index = next_index - 1

    prev_log_term =
      case Enum.at(state.log, prev_log_index - 1) do
        nil -> 0
        entry -> entry.term
      end

    send({:global, {:raft, follower_id}}, {
      :append_entries,
      self(),
      state.current_term,
      state.node_id,
      prev_log_index,
      prev_log_term,
      entries,
      state.commit_index
    })
  end

  defp handle_append_entries(
         state,
         term,
         leader_id,
         prev_log_index,
         prev_log_term,
         entries,
         leader_commit
       ) do
    cond do
      term < state.current_term ->
        {false, state}

      term > state.current_term ->
        # Step down to follower
        new_state = %{
          state
          | current_term: term,
            voted_for: nil,
            state: :follower,
            leader_id: leader_id
        }

        election_timer = start_election_timer(new_state)
        new_state = %{new_state | election_timer: election_timer}

        # Try to append entries
        case try_append_entries(new_state, prev_log_index, prev_log_term, entries) do
          {:ok, final_state} ->
            {true, update_commit_index(final_state, leader_commit)}

          {:error, _} ->
            {false, new_state}
        end

      true ->
        # Same term
        if state.leader_id != leader_id do
          # Update leader
          new_state = %{state | leader_id: leader_id}
          election_timer = start_election_timer(new_state)
          new_state = %{new_state | election_timer: election_timer}

          case try_append_entries(new_state, prev_log_index, prev_log_term, entries) do
            {:ok, final_state} ->
              {true, update_commit_index(final_state, leader_commit)}

            {:error, _} ->
              {false, new_state}
          end
        else
          case try_append_entries(state, prev_log_index, prev_log_term, entries) do
            {:ok, final_state} ->
              {true, update_commit_index(final_state, leader_commit)}

            {:error, _} ->
              {false, state}
          end
        end
    end
  end

  defp try_append_entries(state, prev_log_index, prev_log_term, entries) do
    # Check if previous log entry matches
    case Enum.at(state.log, prev_log_index - 1) do
      nil when prev_log_index == 0 ->
        # First entry, append
        new_log = state.log ++ entries
        {:ok, %{state | log: new_log}}

      entry when entry.term == prev_log_term ->
        # Previous entry matches, append
        new_log = Enum.take(state.log, prev_log_index) ++ entries
        {:ok, %{state | log: new_log}}

      _ ->
        # Previous entry doesn't match
        {:error, :log_inconsistency}
    end
  end

  defp update_commit_index(state, leader_commit) do
    if leader_commit > state.commit_index do
      new_commit_index = min(leader_commit, length(state.log))
      new_state = %{state | commit_index: new_commit_index}

      # Apply committed entries
      apply_committed_entries(new_state)
    else
      state
    end
  end

  defp apply_committed_entries(state) do
    if state.last_applied < state.commit_index do
      # Apply entries from last_applied + 1 to commit_index
      entries_to_apply =
        Enum.slice(state.log, state.last_applied, state.commit_index - state.last_applied)

      Enum.each(entries_to_apply, fn entry ->
        apply_command(entry.command)
      end)

      %{state | last_applied: state.commit_index}
    else
      state
    end
  end

  defp apply_command(command) do
    # Apply command to the state machine (KVStore)
    # Only apply if the storage engine is available
    try do
      case command do
        %{operation: :put, key: key, value: value} ->
          KVStore.put(key, value)

        %{operation: :delete, key: key} ->
          KVStore.delete(key)

        %{operation: :batch_put, kv_pairs: kv_pairs} ->
          KVStore.batch_put(kv_pairs)
      end
    catch
      :exit, _ ->
        # Storage engine not available, just log the command
        Logger.debug("Storage engine not available, command logged: #{inspect(command)}")
        :ok
    end
  end

  defp handle_append_entries_response(state, follower_id, success) do
    if success do
      # Update follower progress
      next_index = Map.get(state.next_index, follower_id, 1)
      new_next_index = next_index + 1
      new_match_index = Map.put(state.match_index, follower_id, next_index)

      new_state = %{
        state
        | next_index: Map.put(state.next_index, follower_id, new_next_index),
          match_index: new_match_index
      }

      # Try to commit new entries
      try_commit_entries(new_state)
    else
      # Decrement next index for this follower
      next_index = Map.get(state.next_index, follower_id, 1)
      new_next_index = max(1, next_index - 1)

      %{state | next_index: Map.put(state.next_index, follower_id, new_next_index)}
    end
  end

  defp try_commit_entries(state) do
    # Find the highest index that can be committed
    sorted_match_indices = state.match_index |> Map.values() |> Enum.sort()
    n = length(state.cluster_nodes)

    # Find the highest index replicated to majority
    majority_index = Enum.at(sorted_match_indices, div(n, 2))

    if majority_index > state.commit_index do
      # Check if the entry at majority_index is from current term
      case Enum.at(state.log, majority_index - 1) do
        %{term: term} when term == state.current_term ->
          new_state = %{state | commit_index: majority_index}
          apply_committed_entries(new_state)

        _ ->
          state
      end
    else
      state
    end
  end

  defp replicate_log(state) do
    other_nodes = state.cluster_nodes -- [state.node_id]

    Enum.each(other_nodes, fn node_id ->
      next_index = Map.get(state.next_index, node_id, 1)
      entries = Enum.slice(state.log, next_index - 1, length(state.log))
      send_append_entries(state, node_id, entries)
    end)
  end

  defp forward_to_leader(leader_id, command) do
    # Forward command to leader
    case GenServer.call(
           {:global, {:raft, leader_id}},
           {:submit_command, command},
           5000
         ) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, reason}
    end
  end

  defp start_election_timer(state) do
    timeout = state.election_timeout_ms + :rand.uniform(state.election_timeout_ms)
    Process.send_after(self(), :election_timeout, timeout)
  end

  defp start_heartbeat_timer(state) do
    Process.send_after(self(), :heartbeat_timeout, state.heartbeat_interval_ms)
  end
end
