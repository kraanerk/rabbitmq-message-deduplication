# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2025, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.CacheManager do
  @moduledoc """
  The Cache Manager takes care of creating, maintaining and destroying caches.
  """

  use GenServer

  require RabbitMQMessageDeduplication.Cache

  alias :timer, as: Timer
  alias :mnesia, as: Mnesia
  alias :mnesia_rocksdb, as: MnesiaRocksdb
  alias :mnesia_rocksdb_admin, as: MrdbAdmin
  alias RabbitMQMessageDeduplication.Cache, as: Cache
  alias RabbitMQMessageDeduplication.Common, as: Common

  Module.register_attribute(__MODULE__,
    :rabbit_boot_step,
    accumulate: true, persist: true)

  @rabbit_boot_step {
    __MODULE__,
    [description: "message deduplication plugin cache maintenance process",
     mfa: {:rabbit_sup, :start_child, [__MODULE__]},
     cleanup: {:rabbit_sup, :stop_child, [__MODULE__]},
     requires: :database,
     enables: :external_infrastructure]}

  def start_link() do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @doc """
  Create the cache and register it within the maintenance process.
  """
  @spec create(atom, boolean, list) :: :ok | { :error, any }
  def create(cache, distributed, options) do
    try do
      timeout = Common.cache_wait_time() + Timer.seconds(5)

      GenServer.call(__MODULE__, {:create, cache, distributed, options}, timeout)
    catch
      :exit, {:noproc, _} -> {:error, :noproc}
    end
  end

  @doc """
  Destroy the cache and remove it from the maintenance process.
  """
  @spec destroy(atom) :: :ok | { :error, any }
  def destroy(cache) do
    try do
      GenServer.call(__MODULE__, {:destroy, cache})
    catch
      :exit, {:noproc, _} -> {:error, :noproc}
    end
  end

  @doc """
  Called by rabbit_node_monitor when a node joins the cluster.
  """
  @spec on_node_up(atom) :: :ok
  def on_node_up(node) do
    GenServer.cast(__MODULE__, {:node_up, node})
  end

  @doc """
  Disable the cache and terminate the manager process.
  """
  def disable() do
    :ok = Supervisor.terminate_child(:rabbit_sup, __MODULE__)
    :ok = Supervisor.delete_child(:rabbit_sup, __MODULE__)
  end

  ## Server Callbacks

  # Run Mnesia creation functions handling output
  defmacro mnesia_create(function) do
    quote do
      case unquote(function) do
        {:atomic, :ok} -> :ok
        {:aborted, {:already_exists, _}} -> :ok
        {:aborted, {:already_exists, _, _}} -> :ok
        error -> error
      end
    end
  end

  # Create the cache table and start the cleanup routine.
  def init(state) do
    Mnesia.start()
    MnesiaRocksdb.register()
    MrdbAdmin.set_and_cache_env(:mnesia_compatible_aborts, true)

    with :ok <- mnesia_create(Mnesia.create_table(caches(), [])),
         :ok <- mnesia_create(Mnesia.add_table_copy(caches(), node(), :ram_copies)),
         :ok <- Mnesia.wait_for_tables([caches()], Common.cache_wait_time())
    do
      Process.send_after(__MODULE__, :cleanup, Common.cleanup_period())
      {:ok, state}
    else
      {:timeout, reason} -> {:error, reason}
      error -> error
    end
  end

  # Create the cache and add it to the Mnesia caches table
  def handle_call({:create, cache, distributed, options}, _from, state) do
    function = fn -> Mnesia.write({caches(), cache, :nil}) end

    with :ok <- Cache.create(cache, distributed, options),
         {:atomic, result} <- Mnesia.transaction(function)
    do
      {:reply, result, state}
    else
      {:aborted, reason} -> {:reply, {:error, reason}, state}
      error -> {:reply, error, state}
    end
  end

  # Drop the cache and remove it from the Mnesia caches table
  def handle_call({:destroy, cache}, _from, state) do
    function = fn -> Mnesia.delete({caches(), cache}) end

    with :ok <- Cache.drop(cache),
         {:atomic, result} <- Mnesia.transaction(function)
    do
      {:reply, result, state}
    else
      {:aborted, reason} -> {:reply, {:error, reason}, state}
      error -> {:reply, error, state}
    end
  end

  # Handle node_up callback from rabbit_node_monitor
  def handle_cast({:node_up, new_node}, state) do
    :rabbit_log.info("Node ~p joined cluster, rebalancing and syncing caches~n", [new_node])

    # Get all caches from the Mnesia registry table
    {:atomic, caches} = Mnesia.transaction(fn -> Mnesia.all_keys(caches()) end)

    # Determine if this node should be the sync coordinator
    # Use the node with the smallest name (lexicographically) to avoid duplicate syncing
    running_nodes = :rabbit_nodes.list_running()
    coordinator = Enum.min(running_nodes -- [new_node])

    if Node.self() == coordinator do
      :rabbit_log.info("This node (~p) is the coordinator, syncing data to ~p~n",
                      [Node.self(), new_node])

      # For each cache, create table on new node and sync data
      for cache <- caches do
        :rabbit_log.info("Creating cache ~p on new node ~p~n", [cache, new_node])

        # Get cache configuration
        distributed = Cache.cache_property(cache, :distributed)
        size = Cache.cache_property(cache, :size)
        ttl = Cache.cache_property(cache, :ttl)

        # Create the cache table directly on the new node
        create_result = :rpc.call(new_node, Cache, :create_local_cache, [cache, distributed, [size: size, ttl: ttl]])

        :rabbit_log.info("Cache creation result for ~p on node ~p: ~p~n", [cache, new_node, create_result])

        case create_result do
          :ok ->
            :rabbit_log.info("Successfully created cache ~p on node ~p, starting sync~n", [cache, new_node])
            # Directly start sync - the table was created successfully
            sync_cache_to_node(cache, new_node)

          {:error, reason} ->
            :rabbit_log.error("Failed to create cache ~p on node ~p: ~p~n", [cache, new_node, reason])

          {:badrpc, reason} ->
            :rabbit_log.error("RPC error creating cache ~p on node ~p: ~p~n", [cache, new_node, reason])

          other ->
            # Handle any other return value (might still be success)
            :rabbit_log.warning("Unexpected result creating cache ~p on node ~p: ~p, attempting sync anyway~n",
                               [cache, new_node, other])
            sync_cache_to_node(cache, new_node)
        end
      end
    else
      :rabbit_log.info("Node ~p is the coordinator, skipping sync from this node (~p)~n",
                      [coordinator, Node.self()])

      # Still need to rebalance to ensure table creation
      for cache <- caches do
        Cache.rebalance_replicas(cache)
      end
    end

    {:noreply, state}
  end

  # The maintenance process deletes expired cache entries.
  def handle_info(:cleanup, state) do
    {:atomic, caches} = Mnesia.transaction(fn -> Mnesia.all_keys(caches()) end)
    Enum.each(caches, &Cache.delete_expired_entries/1)
    Process.send_after(__MODULE__, :cleanup, Common.cleanup_period())

    {:noreply, state}
  end

  # Catch-all for other info messages
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # Sync all cache entries from this node to the target node in batches
  defp sync_cache_to_node(cache, target_node) do
    :rabbit_log.info("Syncing cache ~p to new node ~p~n", [cache, target_node])

    # Spawn a separate process to handle syncing to avoid blocking
    spawn(fn ->
      try do
        sync_cache_entries_batched(cache, target_node)

        # Give the batch insert a moment to complete, then verify
        :timer.sleep(100)

        # Verify sync by checking table size on target node
        case :rpc.call(target_node, :mnesia, :table_info, [cache, :size]) do
          size when is_integer(size) ->
            :rabbit_log.info("Sync verification: cache ~p on node ~p has ~p entries~n",
                            [cache, target_node, size])
          {:badrpc, _reason} ->
            # Table might not be fully ready yet, that's okay
            :ok
          error ->
            :rabbit_log.debug("Could not verify cache ~p size on node ~p: ~p~n",
                             [cache, target_node, error])
        end
      catch
        kind, reason ->
          :rabbit_log.error("Cache sync failed for ~p to node ~p: ~p:~p~n",
                           [cache, target_node, kind, reason])
      end
    end)

    :ok
  end

  # Sync cache entries in batches using continuation-based iteration
  defp sync_cache_entries_batched(cache, target_node) do
    batch_size = 10000
    # Match spec to return all cache entries: {cache_name, key, expiration}
    match_spec = [{{cache, :"$1", :"$2"}, [], [{{:"$1", :"$2"}}]}]

    # Start the select with a limit
    case :mrdb.select(cache, match_spec, batch_size) do
      {entries, continuation} ->
        count = process_and_sync_batch(entries, cache, target_node, 0)
        sync_continuation(continuation, cache, target_node, count)

      :"$end_of_table" ->
        :rabbit_log.info("No entries to sync for cache ~p~n", [cache])
        :ok

      entries when is_list(entries) ->
        count = process_and_sync_batch(entries, cache, target_node, 0)
        :rabbit_log.info("Synced ~p entries to node ~p for cache ~p (completed)~n",
                        [count, target_node, cache])
        :ok
    end
  end

  # Continue processing batches using continuation
  defp sync_continuation(continuation, cache, target_node, total_count) do
    case :mrdb.select(continuation) do
      {entries, new_continuation} ->
        count = process_and_sync_batch(entries, cache, target_node, total_count)
        sync_continuation(new_continuation, cache, target_node, count)

      :"$end_of_table" ->
        :rabbit_log.info("Synced ~p entries to node ~p for cache ~p (completed)~n",
                        [total_count, target_node, cache])
        :ok

      entries when is_list(entries) ->
        count = process_and_sync_batch(entries, cache, target_node, total_count)
        :rabbit_log.info("Synced ~p entries to node ~p for cache ~p (completed)~n",
                        [count, target_node, cache])
        :ok
    end
  end

  # Process and sync a batch of entries
  defp process_and_sync_batch(entries, cache, target_node, current_count) do
    # Collect valid entries with their TTLs
    valid_entries = Enum.reduce(entries, [], fn entry, acc ->
      case entry do
        {entry_key, expiration} ->
          # Calculate remaining TTL
          ttl = case expiration do
            nil -> nil
            exp_time -> max(0, exp_time - :os.system_time(:millisecond))
          end

          # Skip expired entries
          if ttl == nil or ttl > 0 do
            [{entry_key, ttl} | acc]
          else
            acc
          end

        _ ->
          acc
      end
    end)

    # Make one RPC call for the entire batch
    unless Enum.empty?(valid_entries) do
      :rpc.cast(target_node, __MODULE__, :batch_insert, [cache, Enum.reverse(valid_entries)])
    end

    batch_count = length(valid_entries)
    new_count = current_count + batch_count

    if rem(new_count, 10000) == 0 do
      :rabbit_log.info("Synced ~p entries so far for cache ~p to node ~p~n",
                      [new_count, cache, target_node])
    end

    new_count
  end

  @doc """
  Batch insert multiple entries into a cache on the local node.
  Called via RPC from the coordinator node during sync.
  """
  @spec batch_insert(atom, list({any, integer | nil})) :: :ok
  def batch_insert(cache, entries) do
    case Cache.local_batch_insert(cache, entries) do
      {:ok, stats} ->
        :rabbit_log.info("Batch insert completed for cache ~p: inserted=~p, exists=~p~n",
                        [cache, stats.inserted, stats.exists])
        :ok
      {:error, reason} ->
        :rabbit_log.error("Batch insert failed for cache ~p: ~p~n", [cache, reason])
        :ok
    end
  end

  def caches(), do: :message_deduplication_caches
end
