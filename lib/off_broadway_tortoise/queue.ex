defmodule OffBroadwayTortoise.Queue do
  @moduledoc """
  This `Genserver` is used as a message queue to buffer incoming messages.
  The incoming messages are dequeued by broadway whenever demanded by a
  consumer.
  """
  use GenServer

  @callback start(Supervisor.name(), GenServer.name()) ::
              :ok | :ignore | {:error, any}

  @callback enqueue(GenServer.name(), any) :: :ok
  @callback dequeue(GenServer.name(), non_neg_integer) :: [any]

  @doc """
  Starts a supervised queue.
  """
  def start(supervisor, name) do
    case DynamicSupervisor.start_child(supervisor, {__MODULE__, name}) do
      {:error, {:already_started, _}} -> :ok
      {:ok, _} -> :ok
      error -> error
    end
  end

  @doc """
  Starts a queue.
  """
  @spec start_link(GenServer.name()) :: GenServer.on_start()
  def start_link(queue_name) do
    GenServer.start_link(__MODULE__, [], name: queue_name)
  end

  @impl true
  def init(_opts) do
    state = %{
      queue: :queue.new(),
      size: 0
    }

    {:ok, state}
  end

  @doc """
  Stores a new message in the topic queue.
  """
  def enqueue(queue, message) do
    GenServer.call(queue, {:enqueue, message})
  end

  @doc """
  Dequeues messages from a topic queue.
  """
  def dequeue(topic, demand) do
    GenServer.call(topic, {:dequeue, demand})
  end

  @impl true
  def handle_call({:enqueue, msg}, _from, %{queue: queue, size: size} = state) do
    updated_queue = :queue.in(msg, queue)
    new_size = size + 1

    {:reply, :ok, %{state | queue: updated_queue, size: new_size}}
  end

  @impl true
  def handle_call(
        {:dequeue, demand},
        _from,
        %{queue: queue, size: size} = state
      ) do
    {remaining, messages, taken} = take(queue, demand)
    new_size = size - taken

    {:reply, messages, %{state | queue: remaining, size: new_size}}
  end

  defp take(queue, amount), do: do_take(queue, amount, :queue.new(), 0)

  defp do_take(queue, amount, acc, size) when amount > 0 do
    case :queue.out(queue) do
      {{:value, value}, updated_queue} ->
        updated_acc = :queue.in(value, acc)
        do_take(updated_queue, amount - 1, updated_acc, size + 1)

      {:empty, q} ->
        {q, :queue.to_list(acc), size}
    end
  end

  defp do_take(queue, _amount, acc, size),
    do: {queue, :queue.to_list(acc), size}
end
