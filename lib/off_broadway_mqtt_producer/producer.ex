defmodule OffBroadway.MQTTProducer.Producer do
  @moduledoc """
  Acts as Producer for messages from a MQTT topic subscription.

  ## Features

    * Retrying messages based on fail reason in the message status.
    * Telemetry events.
    * Gently handles connection outages thanks to `Tortoise`.
    * Customizeable behaviour by dependency injectoin.

  ## Options

  The producer requires on start a single argument - a list containing as the

    * first element a `t:OffBroadway.MQTTProducer.Config.t/0` struct.  Refere to
      the `OffBroadway.MQTTProducer.Config` module for more info.
    * second element a tuple with the subscription :
      `{:subscription, {"some_topic", 0}}`.

  Any further keywords are passed as options to
  `OffBroadway.MQTTProducer.Client.start/2`.

  ## Notes

    * The buffer queues are started and registered based on topics. If you are
      using shared subscriptiontest you will have a single queue for buffering
      the incoming messages.
    * The default queue keeps buffered messages only in memory. If the queue
      supervisor terminates all unprocessed messages are lost.
    * The buffer queues are supervised independently and don't shut down with
      the producer. That way a restarted producer on the same topic can pick up
      where the faulty one stoped. You might need to stop queues manually if
      stopping a producer on purpose.
  """

  use GenStage

  require Logger

  alias OffBroadway.MQTTProducer
  alias OffBroadway.MQTTProducer.Client
  alias OffBroadway.MQTTProducer.Config

  @behaviour Broadway.Producer

  @typedoc "The internal state of the producer"
  @type state :: %{
          client_id: String.t(),
          config: Config.t(),
          demand: non_neg_integer,
          dequeue_timer: reference,
          queue: GenServer.name()
        }

  @impl true
  @spec init(args) ::
          {:producer, state}
          | {:stop, {:client, :already_started}}
          | {:stop, {:client, :ignore}}
          | {:stop, {:client, term}}
          | {:stop, {:queue, :ignore}}
          | {:stop, {:queue, term}}
        when args: nonempty_improper_list(Config.t(), opts),
             opt: {:subscription, MQTTProducer.subscription()} | Client.option(),
             opts: [opt, ...]
  def init([%Config{} = config, {:subscription, {topic, qos} = sub} | opts]) do
    queue_name = MQTTProducer.queue_name(config, topic)

    :ok =
      config
      |> config_to_metadata()
      |> Keyword.put(:qos, qos)
      |> Keyword.put(:topic, topic)
      |> Logger.metadata()

    client_opts =
      opts
      |> Keyword.put_new_lazy(:client_id, fn ->
        MQTTProducer.unique_client_id(config)
      end)

    with :ok <- start_queue(config, queue_name),
         :ok <- start_client(config, sub, queue_name, client_opts) do
      {:producer,
       %{
         client_id: client_opts[:client_id],
         config: config,
         demand: 0,
         dequeue_timer: nil,
         queue: queue_name
       }}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  defp start_client(
         %{client: client} = config,
         {topic, qos},
         queue_name,
         client_opts
       ) do
    case client.start(config, {topic, qos}, queue_name, client_opts) do
      {:ok, pid} ->
        :ok

      {:error, {:already_started, _}} ->
        {:error, {:client, :already_started}}

      :ignore ->
        {:error, {:client, :ignore}}

      {:error, reason} ->
        {:error, {:client, reason}}
    end
  end

  defp start_queue(config, queue_name) do
    Process.flag(:trap_exit, true)
    child_spec = config.queue.child_spec([config, queue_name])

    case DynamicSupervisor.start_child(config.queue_supervisor, child_spec) do
      {:ok, _} ->
        :ok

      {:error, {:already_started, _}} ->
        topic = MQTTProducer.topic_from_queue_name(queue_name)
        Logger.warn("queue for topic #{inspect(topic)} is already started")
        :ok

      :ignore ->
        {:error, {:queue, :ignore}}

      {:error, reason} ->
        {:error, {:queue, reason}}
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

  @impl true
  def terminate(reason, state) do
    :ok = Tortoise.Connection.disconnect(state.client_id)
    reason
  end

  defp handle_dequeue_messages(
         %{dequeue_timer: nil, demand: demand, config: config} = state
       )
       when demand > 0 do
    messages = dequeue_messages_from_queue(state, demand)
    new_demand = demand - length(messages)

    dequeue_timer =
      case {messages, new_demand} do
        {[], _} -> schedule_dequeue_messages(config.dequeue_interval)
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
         %{queue: queue_name, config: config},
         total_demand
       ) do
    config.queue.dequeue(queue_name, total_demand)
  end

  defp schedule_dequeue_messages(interval) do
    Process.send_after(self(), :dequeue_messages, interval)
  end

  defp config_to_metadata(config) do
    {transport, opts} = config.server

    opts
    |> Keyword.put(:transport, transport)
    |> hide_password
  end

  defp hide_password(meta) do
    if Keyword.has_key?(meta, :password),
      do: Keyword.update!(meta, :password, fn _ -> "******" end),
      else: meta
  end
end
