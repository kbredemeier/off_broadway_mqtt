defmodule OffBroadway.MQTTProducer.Data do
  @moduledoc """
  Defines a data structure for the data passed around in the
  `t:Broadway.Message.t/0` struct.

  * `:acc` - The data that is being processed in a pipeline.
  * `:topic` - A list with the fragments of the topic the data is coming from.
  * `:meta` - A with map arbitrary metadata.
  """

  @type t :: %__MODULE__{
          acc: any,
          topic: nonempty_list(String.t()),
          meta: %{optional(atom) => any}
        }

  defstruct acc: nil, topic: nil, meta: %{}
end
