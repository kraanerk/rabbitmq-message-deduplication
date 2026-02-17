defmodule RabbitMQMessageDeduplication.CacheEvent do
  @moduledoc """
  Event handler for RabbitMQ cluster events related to cache management.

  This module listens to node_up events to handle cache synchronization
  when new nodes join the cluster (particularly for Khepri-based clusters).
  """

  alias :gen_event, as: GenEvent
  alias RabbitMQMessageDeduplication.CacheManager

  @behaviour :gen_event

  Module.register_attribute(__MODULE__,
    :rabbit_boot_step,
    accumulate: true, persist: true)

  @rabbit_boot_step {__MODULE__,
                     [{:description, "message deduplication cache event handler"},
                      {:mfa, {__MODULE__, :add_handler, []}},
                      {:requires, :kernel_ready},
                      {:enables, :core_initialized}]}

  @spec add_handler() :: :ok | {:error, any}
  def add_handler() do
    GenEvent.add_handler(:rabbit_event, __MODULE__, [])
  end

  @spec remove_handler() :: :ok | {:error, any}
  def remove_handler() do
    GenEvent.delete_handler(:rabbit_event, __MODULE__, [])
  end

  @impl :gen_event
  def init(_), do: {:ok, []}

  @impl :gen_event
  def handle_event({:event, :node_added, info, timestamp}, state) do
    send(CacheManager, {:event, :node_added, info, timestamp})
    {:ok, state}
  end

  @impl :gen_event
  def handle_event(_event, state), do: {:ok, state}

  @impl :gen_event
  def handle_call(_request, state), do: {:ok, :not_understood, state}

  @impl :gen_event
  def handle_info(_info, state), do: {:ok, state}

  @impl :gen_event
  def terminate(_arg, _state), do: :ok

  @impl :gen_event
  def code_change(_old_vsn, state, _extra), do: {:ok, state}
end
