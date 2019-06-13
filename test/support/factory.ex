defmodule OffBroadwayTortoise.Factory do
  @moduledoc false

  alias Broadway.Message
  alias OffBroadwayTortoise.Acknowledger
  alias OffBroadwayTortoise.Data

  @doc """
  Builds a data structure equivalent to what the mqtt producer is producing.
  """
  def wrap_data(data, topic) do
    %Data{
      topic: String.split(topic, "/"),
      acc: data
    }
  end

  def wrap_msg(data, queue_name) do
    {:via, _, {_, topic}} = queue_name
    ack_data = %{queue: queue_name, tries: 0}

    %Message{
      data: data,
      metadata: %{},
      acknowledger: {Acknowledger, topic, ack_data}
    }
  end
end
