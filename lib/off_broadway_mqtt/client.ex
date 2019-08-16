defmodule OffBroadway.MQTT.Client do
  @moduledoc """
  Implemens the default MQTT client that is used by the producer to connect
  to the broker.

  The client

    * establishes the connection to OffBroadway.MQTT broker
    * subscribes to the provided topic
    * wrapping the received payload in a `t:OffBroadway.MQTT.Data.t/0`
      struct.
    * enques received and wrapped messages to the passed queue

  This module relies on `Tortoise` and `Tortoise.Supervisor.start_child/1`
  establish the connection and subscribe to the topic.
  """

  require Logger

  alias OffBroadway.MQTT
  alias OffBroadway.MQTT.Config

  @typedoc """
  Type for options for `start/4`.

    * `:client_id` - Can be used to set the client id for the connection. If not
      given a random client id is generated.
    * `sub_ack` - A `t:Process.dest/0` that receives a message as soon as the
      subscription is acknowledged.  The subscriber receives a message in the form
      `{:subscription, client_id, topic, status}`.
  """
  @type option ::
          {:sub_ack, nil | Process.dest()}
          | {:client_id, String.t()}
          | {atom, any}

  @typedoc """
  Collection type for all options that can be passed to `start/4`
  """
  @type options :: [option]

  @doc """
  Starts a MQTT client for the passed subscription with a random client id.

  ## Options

    * `:client_id` - Can be used to set the client id for the connection. If not
      given a random client id is generated.
    * `sub_ack` - A `t:Process.dest/0` that receives a message as soon as the
      subscription is acknowledged.  The subscriber receives a message in the form
      `{:subscription, client_id, topic, status}`.
  """
  @spec start(
          Config.t(),
          MQTT.subscription(),
          MQTT.queue_name(),
          options
        ) :: DynamicSupervisor.on_start_child()
  def start(%Config{} = config, {topic_filter, qos}, queue_name, opts \\ []) do
    client_id =
      Keyword.get_lazy(opts, :client_id, fn ->
        MQTT.unique_client_id(config)
      end)

    warn_if_client_id_is_to_large(client_id)

    {_, server_opts} = server = get_mqtt_server(config)

    meta =
      server_opts
      |> Enum.into(%{})
      |> Map.put(:client_id, client_id)
      |> Map.put(:topic_filter, topic_filter)
      |> Map.put(:qos, qos)
      |> hide_password

    handler_opts =
      Keyword.get_lazy(opts, :handler_opts, fn ->
        [
          client_id: client_id,
          queue: queue_name,
          sub_ack: opts[:sub_ack],
          meta: meta,
          config: config
        ]
      end)

    opts = [
      client_id: client_id,
      handler: {config.handler, handler_opts},
      subscriptions: [{topic_filter, qos}],
      server: server
    ]

    Tortoise.Supervisor.start_child(opts)
  end

  defp warn_if_client_id_is_to_large(client_id)
       when byte_size(client_id) > 23 do
    Logger.warn(
      "Using a client id that is larger than 23 bytes. That might be a" <>
        " problem for the broker you are using!"
    )

    client_id
  end

  defp warn_if_client_id_is_to_large(client_id), do: client_id

  defp hide_password(meta) do
    if Map.has_key?(meta, :password),
      do: Map.update!(meta, :password, fn _ -> "******" end),
      else: meta
  end

  defp get_mqtt_server(%{server: {:ssl, opts}}) do
    {Tortoise.Transport.SSL, opts}
  end

  defp get_mqtt_server(%{server: {:tcp, opts}}) do
    {Tortoise.Transport.Tcp, opts}
  end

  defp get_mqtt_server(%{server: {transport, _}}) do
    raise "invalid transport given for #{__MODULE__}: #{inspect(transport)}" <>
            " Allowed values are `:ssl` and `:tcp`"
  end
end
