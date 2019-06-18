defmodule OffBroadway.MQTTProducer.Error do
  @moduledoc """
  Exceptions can be used to influence the behaviour of the message acknowledger.
  See `OffBroadway.MQTTProducer.Acknowledger` for more details.
  """

  @type t :: %{
          required(:__struct__) => module,
          required(:__exception__) => true,
          optional(:ack) => :retry | :ignore | any,
          optional(:message) => String.t(),
          optional(atom) => any
        }

  defexception message: nil, ack: nil

  def message(e), do: e.message
end
