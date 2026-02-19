# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2025, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.Cache do
  @moduledoc """
  Simple cache implemented on top of Mnesia.

  Entries can be stored within the cache with a given TTL.
  After the TTL expires the entrys will be transparently removed.

  When the cache is full, a random element is removed to make space to a new one.
  A FIFO approach would be preferrable but impractical by now due to Mnesia limitations.

  """
  alias :os, as: Os
  alias :erlang, as: Erlang
  alias :mnesia, as: Mnesia
  alias :mrdb, as: Mrdb
  alias :rabbit_log, as: RabbitLog
  alias RabbitMQMessageDeduplication.Common, as: Common

  @options [:size, :ttl, :distributed, :limit, :default_ttl]

  @doc """
  Create a new cache with the given name and options.

  A distributed cache is replicated across multiple nodes.

  """
  @spec create(atom, boolean, list) :: :ok | { :error, any }
  def create(cache, distributed, options) do
    caches_table = :message_deduplication_caches

    with :ok <- do_cache_create(cache, distributed, options),
         :ok <- register_cache_in_table(caches_table, cache)
    do
      :ok
    else
      {_, reason} -> {:error, reason}
      error -> error
    end
  end

  defp do_cache_create(cache, distributed, options) do
    case cache_create(cache, distributed, options) do
      {_, reason} -> {:error, reason}
      result -> result
    end
  end

  # Register the cache in the caches registry table
  # Waits for the table to be available before writing
  defp register_cache_in_table(caches_table, cache) do
    # Wait for the caches table to be ready
    case Mnesia.wait_for_tables([caches_table], Common.cache_wait_time()) do
      :ok ->
        case Mnesia.transaction(fn -> Mnesia.write({caches_table, cache, :nil}) end) do
          {:atomic, _} -> :ok
          {:aborted, reason} -> {:error, reason}
        end
      {:timeout, _} ->
        # If timeout, try anyway - the table might be available locally
        case Mnesia.transaction(fn -> Mnesia.write({caches_table, cache, :nil}) end) do
          {:atomic, _} -> :ok
          {:aborted, reason} -> {:error, reason}
        end
      error -> error
    end
  end

  @doc """
  Insert the given entry into the cache if it doesn't exist.
  The TTL controls the lifetime in milliseconds of the entry.

  If the cache is full, an entry will be removed to make space.

  """
  @spec insert(atom, any, integer | nil) ::
    { :ok, :inserted | :exists } | { :error, any }
  def insert(cache, entry, ttl \\ nil) do
    RabbitLog.info("Cache insert on node ~p: cache=~p, entry=~p, ttl=~p",
                    [Node.self(), cache, entry, ttl])

    result = local_insert(cache, entry, ttl)

    RabbitLog.info("Cache insert result on node ~p: ~p", [Node.self(), result])

    # Broadcast to other nodes if distributed
    distributed = cache_property(cache, :distributed)
    if result == {:ok, :inserted} and distributed do
      RabbitLog.info("Broadcasting insert for cache ~p to other nodes", [cache])
      broadcast_insert(cache, entry, ttl)
    end

    result
  end

  @doc """
  Insert the given entry into the local cache only (no broadcast).
  """
  @spec local_insert(atom, any, integer | nil) ::
    { :ok, :inserted | :exists } | { :error, any }
  def local_insert(cache, entry, ttl \\ nil) do
    RabbitLog.info("Local insert on node ~p: cache=~p, entry=~p, ttl=~p",
                    [Node.self(), cache, entry, ttl])

    function = fn ->
      if cache_member?(cache, entry) do
        RabbitLog.info("Entry already exists in cache ~p on node ~p", [cache, Node.self()])
        :exists
      else
        RabbitLog.info("Inserting new entry into cache ~p on node ~p", [cache, Node.self()])
        Mrdb.insert(cache, {cache, entry, entry_expiration(cache, ttl)})
        :inserted
      end
    end

    result = case Mrdb.activity(:tx, :rocksdb_copies, function) do
      :exists -> {:ok, :exists}
      :inserted -> {:ok, :inserted}
      {:aborted, reason} -> {:error, reason}
    end

    RabbitLog.info("Local insert result on node ~p: ~p", [Node.self(), result])
    result
  end

  @doc """
  Batch insert multiple entries into the local cache in a single transaction.
  Entries should be a list of {entry_key, ttl} tuples.
  """
  @spec local_batch_insert(atom, list({any, integer | nil})) ::
    { :ok, %{inserted: integer, exists: integer} } | { :error, any }
  def local_batch_insert(cache, entries) do
    RabbitLog.info("Batch insert on node ~p: cache=~p, count=~p",
                    [Node.self(), cache, length(entries)])

    function = fn ->
      Enum.reduce(entries, %{inserted: 0, exists: 0}, fn {entry_key, ttl}, acc ->
        if cache_member?(cache, entry_key) do
          %{acc | exists: acc.exists + 1}
        else
          Mrdb.insert(cache, {cache, entry_key, entry_expiration(cache, ttl)})
          %{acc | inserted: acc.inserted + 1}
        end
      end)
    end

    result = case Mrdb.activity(:tx, :rocksdb_copies, function) do
      %{inserted: _, exists: _} = stats -> {:ok, stats}
      {:aborted, reason} -> {:error, reason}
    end

    RabbitLog.info("Batch insert result on node ~p: ~p", [Node.self(), result])
    result
  end

  @doc """
  Flush the cache content.
  """
  @spec flush(atom) :: :ok | { :error, any }
  def flush(cache) do
    case Mnesia.clear_table(cache) do
      {:atomic, :ok} -> :ok
      {:aborted, reason} -> {:error, reason}
    end
  end

  @doc """
  Drop the cache with all its content.
  """
  @spec drop(atom) :: :ok | { :error, any }
  def drop(cache) do
    case Mnesia.delete_table(cache) do
      {:atomic, :ok} -> :ok
      {:aborted, reason} -> {:error, reason}
    end
  end

  @doc """
  Remove all entries which TTL has expired.
  """
  @spec delete_expired_entries(atom) :: :ok | { :error, any }
  def delete_expired_entries(cache) do
    case Mrdb.select(cache, [{{cache, :"$1", :"$2"},
                              [{:>, Os.system_time(:millisecond), :"$2"}],
                              [:"$1"]}]) do
      expired -> Enum.each(expired, fn e -> Mrdb.delete(cache, e) end)
    end
    :ok
  end

  @doc """
  Return information related to the given cache.
  """
  @spec info(atom) :: list
  def info(cache) do
    with entries when is_integer(entries) <- Mrdb.read_info(cache, :size),
         bytes when is_integer(bytes) <- Mrdb.read_info(cache, :memory)
    do
      {_, nodes} = cache_layout(cache)

      case cache_property(cache, :size) do
        nil -> [entries: entries, bytes: bytes, nodes: nodes]
        size -> [entries: entries, bytes: bytes, nodes: nodes, size: size]
      end
    else
      :undefined -> []
    end
  end

  @doc """
  Rebalance cache replicas.
  """
  @spec rebalance_replicas(atom) :: any
  def rebalance_replicas(cache) do
    if cache_property(cache, :distributed) do
      cache_rebalance(cache)
    end
  end

  @doc """
  Change cache options.
  """
  @spec change_option(atom, atom, any) :: :ok | { :error, any }
  def change_option(cache, option, value) when option in @options do
    :ok = cache_property(cache, option, value)
  end
  def change_option(_, option, _), do: {:error, {:invalid, option}}

  ## Utility functions

  # Mnesia cache table creation.
  defp cache_create(cache, distributed, options) do
    RabbitLog.info("Creating cache ~p on node ~p (distributed: ~p)~n",
      [cache, Node.self(), distributed])

    persistence = :rocksdb_copies
    # RocksDB tables MUST be created locally on each node
    # They cannot be created with a replicas list
    replicas = [Node.self()]
    options = [{:attributes, [:entry, :expiration]},
               {persistence, replicas},
               {:index, [:expiration]},
               {:user_properties, [{:distributed, distributed},
                                   {:size, Keyword.get(options, :size)},
                                   {:ttl, Keyword.get(options, :ttl)},
                                   {:rocksdb_opts, [{:max_open_files, 256*1024},
                                                    {:block_size, 128*1024*1024}]}]}]

    case Mnesia.create_table(cache, options) do
      {:atomic, :ok} ->
        wait_for_cache(cache)
      {:aborted, reason} when elem(reason, 0) == :already_exists ->
        RabbitLog.info("Cache ~p already exists on node ~p~n", [cache, Node.self()])
        maybe_reconfigure(cache, distributed)
      error ->
        error
    end
  end

  # Wait for the table to be loaded and force it in case of timeout
  # For RocksDB tables, Mnesia sync is not required since they're local only
  defp wait_for_cache(cache) do
    case Mnesia.wait_for_tables([cache], Common.cache_wait_time()) do
      {:timeout, [cache]} ->
        Mnesia.force_load_table(cache)
      {:aborted, {:node_not_running, _}} ->
        # RocksDB table was created locally, Mnesia not fully ready yet
        # This is OK for local tables
        RabbitLog.info("Cache ~p created but Mnesia not ready, continuing anyway~n", [cache])
        :ok
      result ->
        result
    end
  end

  # Lookup the entry within the cache.
  # Must be included within transaction.
  defp cache_member?(cache, entry) do
    case Mrdb.read(cache, entry) do
      [_ | _] -> true
      [] -> false
    end
  end

  # Calculate the expiration given a TTL or the cache default TTL
  defp entry_expiration(cache, ttl) do
    default = cache_property(cache, :ttl)

    cond do
      ttl != nil -> Os.system_time(:millisecond) + ttl
      default != nil -> Os.system_time(:millisecond) + default
      true -> nil
    end
  end

  # Retrieve the given property from the Mnesia user_properties field
  @doc false
  def cache_property(cache, property) do
    cache |> Mnesia.table_info(:user_properties) |> Keyword.get(property)
  end

  # Set the given Mnesia user_properties field
  defp cache_property(cache, property, value) when property in @options do
    case Mnesia.write_table_property(cache, {property, value}) do
      {:atomic, :ok} -> :ok
      {:aborted, error} -> {:error, error}
    end
  end

  # Rebalance a distributed cache across the cluster nodes
  defp cache_rebalance(cache) do
    {storage_type, cache_nodes} = cache_layout(cache)

    RabbitLog.info("Rebalancing cache ~p: storage_type=~p, existing_nodes=~p~n",
                   [cache, storage_type, cache_nodes])

    distributed = cache_property(cache, :distributed)

    if storage_type == :rocksdb_copies do
      # For RocksDB tables, we need to create independent copies on each node
      # Get the cache configuration from user properties
      size = cache_property(cache, :size)
      ttl = cache_property(cache, :ttl)

      target_nodes = cache_replicas(cache_nodes)
      RabbitLog.info("Target nodes for cache ~p: ~p~n", [cache, target_nodes])

      for node <- target_nodes do
        if node not in cache_nodes do
          # Check if the cache already exists on the remote node by querying mrdb
          # mrdb.read_info returns :undefined if the table doesn't exist locally
          cache_exists = case :rpc.call(node, :mrdb, :read_info, [cache, :size]) do
            {:badrpc, _} -> false
            :undefined -> false
            size when is_integer(size) -> true
            _ -> false
          end

          if cache_exists do
            RabbitLog.info("Cache ~p already exists on remote node ~p, skipping creation~n", [cache, node])
          else
            RabbitLog.info("Creating cache ~p on remote node ~p~n", [cache, node])
            # Call cache creation on the remote node via RPC
            :rpc.call(node, __MODULE__, :create_local_cache, [cache, distributed, [size: size, ttl: ttl]])
          end
        end
      end
    else
      for node <- cache_replicas(cache_nodes) do
        case Mnesia.add_table_copy(cache, node, storage_type) do
          {:atomic, :ok} ->
            wait_for_cache(cache)
          {:aborted, reason} when elem(reason, 0) == :already_exists ->
            maybe_reconfigure(cache, distributed)
        end
      end
    end
  end

  @doc """
  Create a local RocksDB cache table on the current node.
  This is meant to be called via RPC from other nodes.
  Also registers the cache in the local caches registry table.
  """
  @spec create_local_cache(atom, boolean, list) :: :ok | {:error, any}
  def create_local_cache(cache, distributed, options) do
    RabbitLog.info("Creating local cache ~p on node ~p (distributed: ~p)~n",
                   [cache, Node.self(), distributed])

    # Filter out nil values from options
    options = Enum.filter(options, fn {_k, v} -> v != nil end)

    # Use create to both create the cache and register it in the caches table
    case create(cache, distributed, options) do
      {:error, reason} ->
        RabbitLog.warning("Failed to create local cache ~p: ~p~n", [cache, reason])
        {:error, reason}
      result ->
        RabbitLog.info("Successfully created local cache ~p on node ~p~n", [cache, Node.self()])
        result
    end
  end

  # Broadcast an insert operation to other nodes in the cluster
  defp broadcast_insert(cache, entry, ttl) do
    {storage_type, cache_nodes} = cache_layout(cache)

    RabbitLog.info("Broadcast insert: storage_type=~p, cache_nodes=~p~n",
                   [storage_type, cache_nodes])

    if storage_type == :rocksdb_copies do
      # For RocksDB distributed caches, broadcast to all cluster nodes
      # not just nodes that currently have the table
      target_nodes = cache_replicas(cache_nodes)
      other_nodes = target_nodes -- [Node.self()]

      RabbitLog.info("Broadcasting insert to other nodes: ~p~n", [other_nodes])

      for node <- other_nodes do
        RabbitLog.info("Casting insert to node ~p for cache ~p~n", [node, cache])
        :rpc.cast(node, __MODULE__, :local_insert, [cache, entry, ttl])
      end
    end

    :ok
  end

  # List the nodes on which to create the cache replicas.
  # For RocksDB-based distributed caches, replicate on all cluster nodes.
  defp cache_replicas(cache_nodes \\ []) do
    # Use RabbitMQ's cluster nodes, not Mnesia's, since RabbitMQ 4.x uses Khepri by default
    cluster_nodes = :rabbit_nodes.list_running()

    RabbitLog.info("cache_replicas: cache_nodes=~p, cluster_nodes=~p~n",
                   [cache_nodes, cluster_nodes])

    # Return all cluster nodes, prioritizing existing cache nodes first
    result = cache_nodes ++ (cluster_nodes -- cache_nodes)

    RabbitLog.info("cache_replicas result: ~p~n", [result])

    result
  end

  # Returns a tuple {persistence, nodes}
  defp cache_layout(cache) do
    case Mnesia.table_info(cache, :rocksdb_copies) do
      nodes -> {:rocksdb_copies, nodes}
    end
  end

  # Caches created prior to v0.6.0 need to be reconfigured.
  defp maybe_reconfigure(cache, distributed) do
    if cache_property(cache, :distributed) == nil do
      cache_property(cache, :distributed, distributed)
      cache_property(cache, :size, cache_property(cache, :limit))
      cache_property(cache, :ttl, cache_property(cache, :default_ttl))

      Mnesia.delete_table_property(cache, :limit)
      Mnesia.delete_table_property(cache, :default_ttl)
    end

    wait_for_cache(cache)
  end
end
