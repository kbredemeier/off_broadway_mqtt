defmodule Reflow.PreflightCheck do
  @moduledoc """
  Preflight checks to run before the test suite starts.
  """
  @server_opts :off_broadway_mqtt
               |> Application.get_all_env()
               |> Keyword.get(:server_opts, [])

  require Logger

  alias OffBroadway.MQTT.TestHandler
  alias Tortoise.Connection
  alias Tortoise.Transport.Tcp

  def call do
    wait_for_mqtt()
  end

  defp wait_for_mqtt do
    port = @server_opts[:port] || 1883
    host = @server_opts[:host] || "localhost"
    Logger.info("Waiting for MQTT broker on \"#{host}:#{port}\"...")

    {:ok, _pid} =
      Connection.start_link(
        client_id: "reflow_test",
        server: {Tcp, host: host, port: port},
        handler: {TestHandler, [pid: self()]}
      )

    receive do
      {:test_mqtt_client, :up} -> :ok
    after
      120_000 ->
        raise "MQTT broker on \"#{host}:#{port}\" could not be" <>
                " reached in time!"
    end
  end
end
