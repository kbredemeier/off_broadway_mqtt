defmodule OffBroadway.MQTTProducer.TestHandler do
  @moduledoc false

  use Tortoise.Handler

  def init(opts) do
    {:ok, opts[:pid]}
  end

  def connection(status, test_pid) do
    send(test_pid, {:test_mqtt_client, status})

    {:ok, test_pid}
  end

  def handle_message(_topic, _payload, test_pid) do
    {:ok, test_pid}
  end

  def subscription(_status, _topic_filter, test_pid) do
    {:ok, test_pid}
  end

  def terminate(_reason, _test_pid) do
    :ok
  end
end
