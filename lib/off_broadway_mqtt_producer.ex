defmodule OffBroadway.MQTTProducer do
  @moduledoc """
  A broadway producer for MQTT topic subscriptions.
  """

  alias Broadway.Message
  alias OffBroadway.MQTTProducer.Config

  @type topic :: binary
  @type qos :: 0 | 1 | 2
  @type subscription :: {topic, qos}
  @type queue_name :: {:via, Registry, {atom, topic}}
  @type config :: Config.t()

  defmacro __using__(_) do
    quote do
      use Broadway

      import OffBroadway.MQTTProducer
      require OffBroadway.MQTTProducer

      alias Broadway.Message
      alias OffBroadway.MQTTProducer.Producer
    end
  end

  @doc """
  Rescues any exception and adds it as the fail reason to the message(s).

  Use this macro to wrap the code in your `c:Broadway.handle_message/3` and
  `c:Broadway.handle_batch/4` callback implementations.
  """
  defmacro handle_errors(messages, do: block) do
    quote location: :keep do
      try do
        unquote(block)
      rescue
        e -> fail_msg(unquote(messages), e)
      end
    end
  end

  @doc """
  Adds the second argument as error to the message(s).
  """
  @spec fail_msg([Message.t()], Exception.t()) :: [Message.t()]
  @spec fail_msg(Message.t(), Exception.t()) :: Message.t()
  def fail_msg(messages, exception) when is_list(messages) do
    messages |> Enum.map(&fail_msg(&1, exception))
  end

  def fail_msg(message, exception),
    do: message |> Message.failed(exception)

  @doc """
  Utility function to build a for the running application unique client id that
  can be used when connecting with the broker.

  This ensures that multiple clients from the same application don't kick each
  other from the broker in case the broker does not allow multiple connections
  with the same clent id.
  """
  @spec unique_client_id(:default | config) :: String.t()
  def unique_client_id(config \\ :default)

  def unique_client_id(%{client_id_prefix: prefix}) do
    random = [:positive] |> System.unique_integer() |> to_string
    prefix <> "_" <> random
  end

  def unique_client_id(:default) do
    :default |> Config.new() |> unique_client_id
  end

  @doc """
  Returns the name for the queue belonging to the given topic.
  """
  @spec queue_name(atom, topic) :: {:via, Registry, {atom, topic}}
  def queue_name(
        registry \\ OffBroadway.MQTTProducer.QueueRegistry,
        topic
      )
      when is_binary(topic) and is_atom(registry) do
    {:via, Registry, {registry, topic}}
  end

  @doc """
  Returns the runtime configuration for OffBroadway.MQTTProducer.

  See `f:OffBroadway.MQTTProducer.Config.new/1` for more details.
  """
  @spec config(:default | Config.options()) :: Config.t()
  def config(config_opts \\ :default)
  def config(:default), do: Config.new(:default)

  def config(config_opts) when is_list(config_opts),
    do: Config.new(:default, config_opts)
end
