defmodule OffBroadwayTortoise.Acknowledger do
  @moduledoc """
  Implements `Broadway.Acknowledger` behaviour.

  Special exceptions with a `ack` field can tell the acknowledger to `:ignore`
  or `:retry` a message if it reached the end of the pipeline. For every other
  exception an error is logged with the the exception's `message/1` result as
  message.

  """

  require Logger

  alias OffBroadwayTortoise.Queue

  @behaviour Broadway.Acknowledger

  @type ack_data :: %{queue: GenServer.name(), tries: non_neg_integer}

  @impl Broadway.Acknowledger
  def ack(topic, successful, failed) do
    topic = List.wrap(topic)
    ack_messages(successful, topic, :success)
    ack_messages(failed, topic, :failed)
    :ok
  end

  defp ack_messages(messages, _topic, :failed) do
    Enum.each(messages, fn msg ->
      msg
      |> log_failure
      |> maybe_requeue
    end)

    :ok
  end

  defp ack_messages([], _topic, _status), do: :ok

  defp ack_messages(messages, topic, :success) do
    Logger.debug(
      "Successfully processed #{Enum.count(messages)} messages on #{
        Enum.join(topic, "/")
      }"
    )

    :ok
  end

  defp log_failure(
         %{
           status: {_, %{__exception__: true} = e},
           metadata: metadata,
           data: %{acc: data}
         } = message
       ) do
    log_metadata =
      metadata
      |> Enum.into([])
      |> Keyword.put(:data, data)

    log_failure_for_exception(e, log_metadata)
    message
  end

  defp log_failure(%{
         status: {_, reason},
         metadata: metadata,
         data: %{acc: data}
       }) do
    log_metadata =
      metadata
      |> Enum.into([])
      |> Keyword.put(:data, data)

    Logger.error(
      "Processing message failed with unhandled reason: #{inspect(reason)}",
      log_metadata
    )
  end

  defp log_failure_for_exception(%mod{ack: :ignore} = e, metadata) do
    Logger.debug(mod.message(e), metadata)
  end

  defp log_failure_for_exception(%mod{ack: :retry} = e, metadata) do
    Logger.warn(mod.message(e), metadata)
  end

  defp log_failure_for_exception(%mod{} = e, metadata) do
    Logger.error(mod.message(e), metadata)
  end

  defp maybe_requeue(
         %{acknowledger: {_, _, %{queue: queue}}, status: {_, %{ack: :retry}}} =
           message
       ) do
    updated_message = %{message | status: :ok}
    Queue.enqueue(queue, updated_message)
    updated_message
  end

  defp maybe_requeue(msg), do: msg
end
