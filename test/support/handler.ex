defmodule OffBroadway.MQTT.TestHandler do
  @moduledoc false

  use Tortoise.Handler

  def init(opts) do
    {:ok, opts}
  end

  def connection(status, opts) do
    send(opts[:pid], {:test_mqtt_client, status})

    {:ok, opts}
  end

  def handle_message(_topic, _payload, opts) do
    {:ok, opts}
  end

  def subscription(_status, _topic_filter, opts) do
    {:ok, opts}
  end

  def terminate(_reason, _opts) do
    :ok
  end
end
