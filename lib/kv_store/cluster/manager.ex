defmodule KVStore.Cluster.Manager do
  @moduledoc """
  Cluster manager for coordinating Raft nodes and providing cluster-wide operations.
  """

  use GenServer
  require Logger

  defstruct [
    :node_id,
    :cluster_nodes,
    :raft_pid,
    :enabled
  ]

  # Client API

  @doc """
  Start the cluster manager.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Get cluster status.
  """
  def cluster_status do
    GenServer.call(__MODULE__, :cluster_status)
  end

  @doc """
  Submit a command to the cluster.
  """
  def submit_command(command) do
    GenServer.call(__MODULE__, {:submit_command, command})
  end

  @doc """
  Get the current leader.
  """
  def get_leader do
    GenServer.call(__MODULE__, :get_leader)
  end

  @doc """
  Add a node to the cluster.
  """
  def add_node(node_id) do
    GenServer.call(__MODULE__, {:add_node, node_id})
  end

  @doc """
  Remove a node from the cluster.
  """
  def remove_node(node_id) do
    GenServer.call(__MODULE__, {:remove_node, node_id})
  end

  # GenServer Callbacks

  @impl true
  def init(_opts) do
    config = KVStore.Config.cluster_config()

    if config[:enabled] do
      node_id = config[:node_id]
      cluster_nodes = config[:cluster_nodes]

      # Start Raft node
      raft_opts = [
        node_id: node_id,
        cluster_nodes: cluster_nodes,
        election_timeout_ms: config[:election_timeout_ms],
        heartbeat_interval_ms: config[:heartbeat_interval_ms]
      ]

      {:ok, raft_pid} = KVStore.Cluster.Raft.start_link(raft_opts)

      state = %__MODULE__{
        node_id: node_id,
        cluster_nodes: cluster_nodes,
        raft_pid: raft_pid,
        enabled: true
      }

      Logger.info("Cluster manager started for node #{node_id}")
      {:ok, state}
    else
      Logger.info("Cluster manager disabled")
      {:ok, %__MODULE__{enabled: false}}
    end
  end

  @impl true
  def handle_call(:cluster_status, _from, state) do
    if state.enabled do
      status = KVStore.Cluster.Raft.cluster_status(state.node_id)
      {:reply, status, state}
    else
      {:reply, %{enabled: false}, state}
    end
  end

  @impl true
  def handle_call({:submit_command, command}, _from, state) do
    if state.enabled do
      result = KVStore.Cluster.Raft.submit_command(state.node_id, command)
      {:reply, result, state}
    else
      {:reply, {:error, :cluster_disabled}, state}
    end
  end

  @impl true
  def handle_call(:get_leader, _from, state) do
    if state.enabled do
      status = KVStore.Cluster.Raft.cluster_status(state.node_id)
      leader = status.leader_id
      {:reply, leader, state}
    else
      {:reply, nil, state}
    end
  end

  @impl true
  def handle_call({:add_node, _node_id}, _from, state) do
    if state.enabled do
      # This would require more complex cluster membership management
      # For now, we'll just return an error
      {:reply, {:error, :not_implemented}, state}
    else
      {:reply, {:error, :cluster_disabled}, state}
    end
  end

  @impl true
  def handle_call({:remove_node, _node_id}, _from, state) do
    if state.enabled do
      # This would require more complex cluster membership management
      # For now, we'll just return an error
      {:reply, {:error, :not_implemented}, state}
    else
      {:reply, {:error, :cluster_disabled}, state}
    end
  end
end
