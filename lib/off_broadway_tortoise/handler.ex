defmodule OffBroadwayTortoise.Handler do
  @moduledoc """
  `Tortoise.Handler` implementation.
  Incoming messages are wrapped in `t:Broadway.Messate.t/0` structs and pushed
  into the provided queue.
  """

  use Tortoise.Handler

  require Logger

  alias Broadway.Message
  alias OffBroadwayTortoise.Acknowledger
  alias OffBroadwayTortoise.Data
  alias OffBroadwayTortoise.Queue

  defstruct queue: nil, acknowledger: nil, meta: nil, sub_ack: nil

  def init(opts) do
    queue_name = Keyword.fetch!(opts, :queue)
    acknowledger = Keyword.get(opts, :acknowledger) || Acknowledger
    meta = Keyword.get(opts, :meta) || %{}
    Logger.debug("initializing client", Enum.into(meta, []))

    {:ok,
     %__MODULE__{
       meta: meta,
       queue: queue_name,
       acknowledger: acknowledger,
       sub_ack: opts[:sub_ack]
     }}
  end

  def connection(_status, state) do
    Logger.debug("client attempting to connect", Enum.into(state.meta, []))

    {:ok, state}
  end

  def handle_message(topic, payload, %{queue: queue} = state) do
    message = wrap_message(state, topic, payload)

    :ok = Queue.enqueue(queue, message)
    {:ok, state}
  end

  def subscription(status, topic_filter, %{sub_ack: nil} = state) do
    Logger.debug(
      "client subscription #{status} on #{topic_filter}",
      Enum.into(state.meta, [])
    )

    {:ok, state}
  end

  def subscription(
        status,
        topic_filter,
        %{sub_ack: sub_ack, meta: %{client_id: client_id}} = state
      ) do
    Logger.debug(
      "client subscription #{status} on #{topic_filter}",
      Enum.into(state.meta, [])
    )

    send(sub_ack, {:subscription, client_id, topic_filter, status})
    {:ok, state}
  end

  def terminate(_reason, _state) do
    :ok
  end

  defp wrap_message(
         %{meta: meta, acknowledger: acknowledger, queue: queue},
         topic,
         payload
       ) do
    ack_data = %{queue: queue, tries: 0}

    %Message{
      data: %Data{topic: topic, acc: payload},
      metadata: Map.merge(meta, %{topic: topic}),
      acknowledger: {acknowledger, topic, ack_data}
    }
  end
end
