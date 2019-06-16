defmodule OffBroadway.MQTTProducer.Handler do
  @moduledoc """
  Default `Tortoise.Handler` implementation used by
  `OffBroadway.MQTTProducer.Client`.

  Incoming messages are wrapped in `t:Broadway.Messate.t/0` structs and pushed
  into the provided queue.
  """

  use Tortoise.Handler

  require Logger

  alias Broadway.Message
  alias OffBroadway.MQTTProducer.Data
  alias OffBroadway.MQTTProducer.Telemetry

  defstruct queue: nil,
            config: nil,
            meta: nil,
            sub_ack: nil,
            client_id: nil

  def init(opts) do
    config = Keyword.fetch!(opts, :config)
    queue_name = Keyword.fetch!(opts, :queue)
    client_id = Keyword.fetch!(opts, :client_id)
    meta = Keyword.get(opts, :meta) || %{}
    Logger.debug("initializing client", Enum.into(meta, []))

    {:ok,
     %__MODULE__{
       config: config,
       meta: meta,
       client_id: client_id,
       queue: queue_name,
       sub_ack: opts[:sub_ack]
     }}
  end

  def connection(status, %{config: config} = state) do
    Logger.debug("client attempting to connect", Enum.into(state.meta, []))

    Telemetry.client_connection_status(config, status, state.meta)

    {:ok, state}
  end

  def handle_message(topic, payload, %{config: config, queue: queue} = state) do
    message = wrap_message(state, topic, payload)
    :ok = config.queue.enqueue(queue, message)

    Telemetry.client_message_received(config, state.meta)

    {:ok, state}
  end

  def subscription(status, topic_filter, %{config: config} = state) do
    Logger.debug(
      "client subscription #{status} on #{topic_filter}",
      Enum.into(state.meta, [])
    )

    Telemetry.client_subscription_status(config, status, state.meta)

    {:ok, maybe_suback(state, topic_filter, status)}
  end

  defp maybe_suback(%{sub_ack: nil} = state, _, _), do: state

  defp maybe_suback(
         %{sub_ack: dest, client_id: client_id} = state,
         topic_filter,
         status
       ) do
    if Process.alive?(dest),
      do: send(dest, {:subscription, client_id, topic_filter, status})

    state
  end

  def terminate(_reason, _state) do
    :ok
  end

  defp wrap_message(
         %{meta: meta, config: config, queue: queue},
         topic,
         payload
       ) do
    ack_data = %{queue: queue, tries: 0, config: config}
    topic_str = Enum.join(topic, "/")

    %Message{
      data: %Data{topic: topic, acc: payload},
      metadata: Map.merge(meta, %{topic: topic_str}),
      acknowledger: {config.acknowledger, topic_str, ack_data}
    }
  end
end
