defmodule OffBroadwayTortoise.Client do
  @moduledoc """
  Implemens the default MQTT client that is used by the producer to connect
  to the broker.

  Broker.

  The client
  * establishes the connection to OffBroadwayTortoise broker.
  * wrapping the received payload in a `t:OffBroadwayTortoise.Data.t/0` struct.
  * enques received and wrapped messages to the passed queue
  """

  require Logger

  alias OffBroadwayTortoise
  alias OffBroadwayTortoise.Handler

  @type option ::
          {:handler, {module, keyword}}
          | {:sub_ack, nil | GenServer.name()}
          | {atom, any}

  @type options :: [option]

  @callback start(
              queue_name :: GenServer.name(),
              OffBroadwayTortoise.subscriptions(),
              OffBroadwayTortoise.conn() | nil,
              options
            ) :: :ok

  @doc """
  Starts a MQTT client for the passed subscription. If given multiple
  subscriptions it stars one client for each subscription.
  """
  def start(queue, subscriptions, conn \\ nil, opts \\ [])

  def start(queue, {topic, qos}, conn, opts) do
    mqtt_config = OffBroadwayTortoise.config()

    client_id = OffBroadwayTortoise.unique_client_id(mqtt_config)

    {_, conn_opts} = server = conn || get_mqtt_conn(mqtt_config)

    {handler_mod, handler_opts} = opts[:handler] || {Handler, []}

    handler_opts =
      handler_opts
      |> Keyword.put_new(:queue, queue)
      |> Keyword.put_new(:sub_ack, opts[:sub_ack])
      |> Keyword.put_new_lazy(:meta, fn ->
        conn_opts
        |> Enum.into(%{})
        |> Map.put(:client_id, client_id)
        |> Map.put(:topic, topic)
        |> Map.put(:qos, qos)
      end)

    opts = [
      client_id: client_id,
      handler: {handler_mod, handler_opts},
      subscriptions: [{topic, qos}],
      server: server
    ]

    case Tortoise.Supervisor.start_child(opts) do
      {:error, {:already_started, _pid}} ->
        Logger.warn("client already started", handler_opts[:meta])
        :ok

      {:ok, _} ->
        :ok

      {:error, reason} ->
        raise ArgumentError,
              "invalid args given to #{__MODULE__}.start/4, #{inspect(reason)}"
    end
  end

  def start(queue, subscriptions, conn, opts) when is_list(subscriptions) do
    Enum.each(subscriptions, &start(queue, &1, conn, opts))
  end

  defp get_mqtt_conn(%{conn: {:ssl, opts}}) do
    {Tortoise.Transport.SSL, opts}
  end

  defp get_mqtt_conn(%{conn: {:tcp, opts}}) do
    {Tortoise.Transport.Tcp, opts}
  end

  defp get_mqtt_conn(%{conn: {transport, _}}) do
    raise "invalid transport given for #{__MODULE__}: #{inspect(transport)}" <>
            " Allowed values are `:ssl` and `:tcp`"
  end
end
