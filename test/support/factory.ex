defmodule OffBroadway.MQTT.Factory do
  @moduledoc false

  alias Broadway.Message
  alias OffBroadway.MQTT.Acknowledger
  alias OffBroadway.MQTT.Config
  alias OffBroadway.MQTT.Data

  @doc """
  Builds a data structure equivalent to what the mqtt producer is producing.
  """
  def wrap_data(data, topic) do
    %Data{
      topic: String.split(topic, "/"),
      acc: data
    }
  end

  def wrap_msg(data, queue_name, config \\ nil) do
    {:via, _, {_, topic}} = queue_name

    ack_data = %{
      queue: queue_name,
      tries: 0,
      config: config || Config.new_from_app_config()
    }

    %Message{
      data: data,
      metadata: %{},
      acknowledger: {Acknowledger, topic, ack_data}
    }
  end
end
