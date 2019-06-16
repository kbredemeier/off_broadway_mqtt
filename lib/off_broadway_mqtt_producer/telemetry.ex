defmodule OffBroadway.MQTTProducer.Telemetry do
  @moduledoc false

  @enabled Application.get_env(
             :off_broadway_mqtt_producer,
             :telemetry_enabled,
             false
           )

  if @enabled do
    def client_connection_status(%{telemetry_prefix: prefix}, status, meta) do
      :telemetry.execute(
        [prefix, :client, :connection, status],
        %{count: 1},
        get_meta(meta)
      )
    end

    def client_message_received(%{telemetry_prefix: prefix}, meta) do
      :telemetry.execute(
        [prefix, :client, :messages],
        %{count: 1},
        get_meta(meta)
      )
    end

    def client_subscription_status(%{telemetry_prefix: prefix}, status, meta) do
      :telemetry.execute(
        [prefix, :client, :subscription, status],
        %{count: 1},
        get_meta(meta)
      )
    end

    def queue_in(%{telemetry_prefix: prefix}, new_size, meta) do
      :telemetry.execute(
        [prefix, :queue, :in],
        %{count: 1, size: new_size},
        get_meta(meta)
      )
    end

    def queue_out(%{telemetry_prefix: prefix}, taken, new_size, meta) do
      :telemetry.execute(
        [prefix, :queue, :out],
        %{count: taken, size: new_size},
        get_meta(meta)
      )
    end

    def acknowledger_status(%{telemetry_prefix: prefix}, status, meta) do
      :telemetry.execute(
        [prefix, :acknowledger, suffix_from_status(status)],
        %{count: 1},
        meta
      )
    end

    defp suffix_from_status(:ok), do: :success
    defp suffix_from_status({:failed, %{ack: :ignore}}), do: :ignored
    defp suffix_from_status({:failed, %{ack: :retry}}), do: :requeued
    defp suffix_from_status({:failed, %{ack: ack}}), do: ack
    defp suffix_from_status({:failed, _}), do: :failed

    defp get_meta(fun) when is_function(fun, 0), do: fun.()
    defp get_meta(meta), do: meta
  else
    def client_connection_status(_, _, _), do: :ok
    def client_message_received(_, _), do: :ok
    def client_subscription_status(_, _, _), do: :ok
    def queue_in(_, _, _), do: :ok
    def queue_out(_, _, _, _), do: :ok
    def acknowledger_status(_, _, _), do: :ok
  end
end
