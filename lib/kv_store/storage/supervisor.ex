defmodule KVStore.Storage.Supervisor do
  @moduledoc """
  Supervisor for the storage engine components.

  Manages:
  - Storage.Engine: Main storage engine GenServer
  - Storage.FileCache: LRU cache for open file handles
  - Storage.Compactor: Background compaction process
  """

  use Supervisor

  def start_link(opts) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      # Main storage engine
      {KVStore.Storage.Engine, KVStore.Config.storage_config()},
      # File handle cache
      {KVStore.Storage.FileCache, KVStore.Config.file_cache_config()},
      # Background compactor
      {KVStore.Storage.Compactor, KVStore.Config.compactor_config()},
      # Durability manager
      {KVStore.Storage.Durability, KVStore.Config.durability_config()},
      # Read-ahead cache
      {KVStore.Storage.Cache, KVStore.Config.cache_config()}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
