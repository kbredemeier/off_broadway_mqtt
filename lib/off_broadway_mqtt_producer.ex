defmodule OffBroadway.MQTTProducer do
  @moduledoc """
  A broadway producer for MQTT topic subscriptions.
  """

  @type topic :: binary
  @type qos :: 0 | 1 | 2
  @type subscriptions :: [{topic, qos}]

  @type conn_opt ::
          {:host, charlist}
          | {:port, non_neg_integer}
          | {atom, any}

  @type conn_opts :: [conn_opt, ...]

  @type conn :: {:tcp | :ssl, conn_opts}

  @type config :: %{
          client_id_prefix: String.t(),
          conn: conn
        }

  @default_transport :tcp
  @default_host 'localhost'
  @default_port 1883
  @default_client_id_prefix "off_broadway_mqtt_producer"

  alias Broadway.Message

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
  @spec unique_client_id(nil | config) :: String.t()
  def unique_client_id(config \\ nil) do
    config = config || config()
    random = [:positive] |> System.unique_integer() |> to_string
    config.client_id_prefix <> "_" <> random
  end

  @doc """
  Returns the name for queue belonging to the given topic.
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
  """
  @spec config(nil | keyword) :: config
  def config(opts \\ nil) do
    config = opts || Application.get_all_env(:off_broadway_mqtt_producer)
    client_id_prefix = config[:client_id_prefix] || @default_client_id_prefix

    {transport, conn_opts} =
      config
      |> Keyword.get(:connection, [])
      |> case do
        conn_opts when is_list(conn_opts) -> conn_opts
        _ -> []
      end
      |> Keyword.update(:host, @default_host, &parse_host/1)
      |> Keyword.update(:port, @default_port, &parse_port/1)
      |> Keyword.pop(:transport, @default_transport)

    %{
      client_id_prefix: client_id_prefix,
      conn: {transport, conn_opts}
    }
  end

  defp parse_host(host) when is_binary(host), do: String.to_charlist(host)
  defp parse_host(host), do: host

  defp parse_port(port) when is_integer(port), do: port

  defp parse_port(port) when is_binary(port) do
    case Integer.parse(port) do
      {port, ""} -> port
      _ -> raise "invalid port configured for #{__MODULE__}: #{port}"
    end
  end

  defp parse_port(port),
    do: raise("invalid port configured for #{__MODULE__}: #{port}")
end
