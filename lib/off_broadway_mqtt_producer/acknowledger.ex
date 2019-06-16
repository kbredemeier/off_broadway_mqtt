defmodule OffBroadway.MQTTProducer.Acknowledger do
  @moduledoc """
  Implements `Broadway.Acknowledger` behaviour.

  Special exceptions with a `ack` field can tell the acknowledger to `:ignore`
  or `:retry` a message if it reached the end of the pipeline. For every other
  exception an error is logged with the the exception's `message/1` result as
  message.

  """

  require Logger

  alias OffBroadway.MQTTProducer.Config

  @behaviour Broadway.Acknowledger

  @type ack_data :: %{
          queue: GenServer.name(),
          tries: non_neg_integer,
          config: Config.t()
        }

  @impl Broadway.Acknowledger
  def ack(topic, successful, failed) do
    Logger.metadata(topic: topic)
    ack_messages(successful, topic, :success)
    ack_messages(failed, topic, :failed)
    :ok
  end

  defp ack_messages(messages, _topic, :failed) do
    Enum.each(messages, fn msg ->
      msg
      |> send_telemetry_event()
      |> log_failure
      |> maybe_requeue
    end)

    :ok
  end

  defp ack_messages([], _topic, _status), do: :ok

  defp ack_messages(messages, topic, :success) do
    Enum.each(messages, &send_telemetry_event/1)

    Logger.debug(
      "Successfully processed #{Enum.count(messages)} messages on #{
        inspect(topic)
      }"
    )

    :ok
  end

  defp log_failure(
         %{
           status: {_, %{__exception__: true} = e},
           metadata: metadata
         } = message
       ) do
    log_metadata =
      metadata
      |> Enum.into([])

    log_failure_for_exception(e, log_metadata)
    message
  end

  defp log_failure(%{
         status: {_, reason},
         metadata: metadata
       }) do
    log_metadata =
      metadata
      |> Enum.into([])

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
         %{
           acknowledger: {_, _, %{config: config, queue: queue_name}},
           status: {_, %{ack: :retry}}
         } = message
       ) do
    updated_message = %{message | status: :ok}
    config.queue.enqueue(queue_name, updated_message)
    updated_message
  end

  defp maybe_requeue(msg), do: msg

  defp send_telemetry_event(
         %{
           acknowledger: {_, _, %{config: config}},
           status: status,
           metadata: metadata
         } = message
       ) do
    :telemetry.execute(
      [config.telemetry_prefix, :acknowledger, suffix_from_status(status)],
      %{count: 1},
      metadata
    )

    message
  end

  defp suffix_from_status(:ok), do: :success
  defp suffix_from_status({:failed, %{ack: :ignore}}), do: :ignored
  defp suffix_from_status({:failed, %{ack: :retry}}), do: :requeued
  defp suffix_from_status({:failed, _}), do: :failed
end
