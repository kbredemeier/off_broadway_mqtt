defmodule OffBroadway.MQTTProducer.Client do
  @moduledoc """
  Implemens the default MQTT client that is used by the producer to connect
  to the broker.

  The client

    * establishes the connection to OffBroadway.MQTTProducer broker
    * subscribes to the provided topic
    * wrapping the received payload in a `t:OffBroadway.MQTTProducer.Data.t/0`
      struct.
    * enques received and wrapped messages to the passed queue

  This module relies on `Tortoise` and `f:Tortoise.Supervisor.start_child/1`
  establish the connection and subscribe to the topic.
  """

  require Logger

  alias OffBroadway.MQTTProducer
  alias OffBroadway.MQTTProducer.Config

  @type option ::
          {:handler_opts, keyword}
          | {:sub_ack, nil | Process.dest()}
          | {:client_id, String.t()}
          | {atom, any}

  @type options :: [option]

  @doc """
  Starts a MQTT client for the passed subscription with a random client id.

  When passing `:default` as the first argument
  `f:OffBroadway.MQTTProducer.config/1` is used to figure out the connection.

  ## Options

    * `:client_id` - Can be used to set the client id for the connection. If not
      given a random client id is generated.
    * `:sub_ack` - Can be used to inject a subscriber for subscription events.
      The subscriber receives a message in the form
      `{:subscription, client_id, topic, status}`.
  """
  @spec start(
          Config.t() | Conifig.options() | :default,
          MQTTProducer.subscription(),
          MQTTProducer.queue_name(),
          options
        ) :: DynamicSupervisor.on_start_child()
  def start(config \\ :default, subscription, queue_name, opts \\ [])

  def start(arg, subscription, queue_name, opts)
      when arg == :default
      when is_list(arg) do
    arg
    |> Config.new()
    |> start(subscription, queue_name, opts)
  end

  def start(%Config{} = config, {topic_sub, qos}, queue_name, opts) do
    client_id =
      Keyword.get_lazy(opts, :client_id, fn ->
        MQTTProducer.unique_client_id(config)
      end)

    {_, server_opts} = server = get_mqtt_server(config)

    meta =
      server_opts
      |> Enum.into(%{})
      |> Map.put(:client_id, client_id)
      |> Map.put(:topic_sub, topic_sub)
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
      subscriptions: [{topic_sub, qos}],
      server: server
    ]

    Tortoise.Supervisor.start_child(opts)
  end

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
