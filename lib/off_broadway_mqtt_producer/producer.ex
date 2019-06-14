defmodule OffBroadway.MQTTProducer.Producer do
  @moduledoc """
  Acts as Producer for messages from mqtt a mqtt topic subscription.
  A produce connects to the given topic with with given QOS level and produces
  `t:Broadway.Message.t/0` events from the incoming messages.

  Internally this producer
  * implements the `GenStage` behaviour as a producer
  * starts a mqtt client and subscribes it to the given topic
  * and starts a queue that serves as buffer for incoming messages
  * dequeues messages if receiving demand from a consumer

  Whenever a consumer demands messages from the producer it dequeues them and
  passes them to the consumer.
  """

  use GenStage

  require Logger

  alias OffBroadway.MQTTProducer

  @behaviour Broadway.Producer

  @default_dequeue_interval 5000

  @default_queue OffBroadway.MQTTProducer.Queue
  @default_queue_registry OffBroadway.MQTTProducer.QueueRegistry
  @default_queue_supervisor OffBroadway.MQTTProducer.QueueSupervisor
  @default_mqtt_client OffBroadway.MQTTProducer.Client

  @type state :: %{
          demand: non_neg_integer,
          dequeue_timer: non_neg_integer,
          dequeue_interval: nil | reference,
          queue: {module, GenServer.name()}
        }

  @impl true
  @spec init(options) :: {:ok, state}
        when option:
               {:dequeue_interval, non_neg_integer}
               | {:queue, module}
               | {:registry, GenServer.name()}
               | {:supervisor, GenServer.name()}
               | {:client, module}
               | {:topic, MQTTProducer.topic()}
               | {:qos, MQTTProducer.qos()}
               | {:connection, {atom, [term]}}
               | {:sub_ack, Process.dest()},
             options: [option, ...]
  def init(opts) do
    dequeue_interval = opts[:dequeue_interval] || @default_dequeue_interval
    queue = opts[:queue] || @default_queue
    registry = opts[:registry] || @default_queue_registry
    supervisor = opts[:supervisor] || @default_queue_supervisor

    client = opts[:client] || @default_mqtt_client
    topic = opts[:topic]
    qos = opts[:qos] || 0
    conn = opts[:connection] || :default
    client_opts = Keyword.take(opts, [:sub_ack])

    queue_name = MQTTProducer.queue_name(registry, topic)

    with :ok <- queue.start(supervisor, queue_name),
         {:ok, _} <- client.start(conn, {topic, qos}, queue_name, client_opts) do
      {:producer,
       %{
         demand: 0,
         dequeue_timer: nil,
         dequeue_interval: dequeue_interval,
         queue: {queue, queue_name}
       }}
    else
      {:error, reason} ->
        raise ArgumentError,
              "invalid options given to #{inspect(client)}.init/1, " <>
                "#{inspect(reason)}"
    end
  end

  @impl true
  def handle_demand(incoming_demand, %{demand: demand} = state) do
    handle_dequeue_messages(%{state | demand: incoming_demand + demand})
  end

  @impl true
  def handle_info(:dequeue_messages, state) do
    handle_dequeue_messages(%{state | dequeue_timer: nil})
  end

  @impl true
  def handle_info(_, state) do
    {:noreply, [], state}
  end

  defp handle_dequeue_messages(%{dequeue_timer: nil, demand: demand} = state)
       when demand > 0 do
    messages = dequeue_messages_from_queue(state, demand)
    new_demand = demand - length(messages)

    dequeue_timer =
      case {messages, new_demand} do
        {[], _} -> schedule_dequeue_messages(state.dequeue_interval)
        {_, 0} -> nil
        _ -> schedule_dequeue_messages(0)
      end

    {:noreply, messages,
     %{state | demand: new_demand, dequeue_timer: dequeue_timer}}
  end

  defp handle_dequeue_messages(state) do
    {:noreply, [], state}
  end

  defp dequeue_messages_from_queue(
         %{queue: {queue, name}},
         total_demand
       ) do
    queue.dequeue(name, total_demand)
  end

  defp schedule_dequeue_messages(interval) do
    Process.send_after(self(), :dequeue_messages, interval)
  end
end
