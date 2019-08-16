defmodule OffBroadway.MQTT.Config do
  @default_transport :tcp
  @default_host "localhost"
  @default_port 1883
  @default_dequeue_interval 5000
  @default_client_id_prefix "obmp"
  @default_supervisor OffBroadway.MQTT.QueueSupervisor
  @default_registry OffBroadway.MQTT.QueueRegistry
  @default_telemetry_prefix :off_broadway_mqtt

  @moduledoc """
  Defines a data structure for configuring this library.

  ## Config options

    * `dequeue_interval` - The interval used by the producer to timout polls to
      the queue process. Defaults to `#{inspect(@default_dequeue_interval)}`.
    * `client_id_prefix` - The value is used to prefix the randomly generated
      client ids by the MQTT client. Defaults to
      #{inspect(@default_client_id_prefix)}. Keep in mind that some brokers
      limit the size  of client_id size to 23 bytes!
    * `server_opts` - See the "Server options" section for details.
    * `telemetry_prefix` - Sets the prefix for any telemery events. Defaults to
      `#{inspect(@default_telemetry_prefix)}`.

  ### Server options

  All options given with the `server_opts` option are passed to `Tortoise` when
  starting the connection process. The following options can be given:

    * `host` - The host the MQTT client uses by default. Defaults to
      `#{inspect(@default_host)}`.
    * `port` - The port the MQTT client uses by default. Defaults to
      `#{inspect(@default_port)}`.
    * `transport` - The protocol the MQTT client uses by default. Defaults to
      `#{inspect(@default_transport)}`.
    * See `Tortoise.Connection.start_link/1` for further options.

  ## Dependency injection

  Besides the other config options it is also possible to replace some modules
  used by providing an alternative implementation:

    * `queue_supervisor` - The supervisor that supervises the queue processes
      started by the producer. Defaults to `#{inspect(@default_supervisor)}`.
    * `queue_registry` - The registry that is used to register the queue
      processes started by the producer. Defaults to
      `#{inspect(@default_registry)}`.
    * `acknowledger` - The `Broadway.Acknowledger` implementation used when
      building the message strucs.
    * `client` - The MQTT client module.
    * `handler`- The handler module used by the default client.
    * `queue` - The queue used in the handler and producer to enqueue / dequeue
    messages.


  ## Compiletime configuration

  The following options must be given at compile time:

  * `telemetry_enabled` - Enables telemetry events if set to true. This option
    is disabled by default.

  ## Configuration example

      use Mix.Config

      config :off_broadway_mqtt,
        client_id_prefix: "sensor_data_processor",
        server_opts: [
          host: "vernemq",
          port: 8883,
          transport: :ssl
        ],
        handler: MyApp.BetterHandler

  ## Building configurations

      iex> OffBroadway.MQTT.Config.new()
      %OffBroadway.MQTT.Config{
        acknowledger: OffBroadway.MQTT.Acknowledger,
        client: OffBroadway.MQTT.Client,
        client_id_prefix: "obmp",
        dequeue_interval: 5000,
        handler: OffBroadway.MQTT.Handler,
        producer: OffBroadway.MQTT.Producer,
        queue: OffBroadway.MQTT.Queue,
        queue_registry: OffBroadway.MQTT.QueueRegistry,
        queue_supervisor: OffBroadway.MQTT.QueueSupervisor,
        server: {:tcp, [host: 'localhost', port: 1883]},
        telemetry_prefix: :off_broadway_mqtt
      }

      iex> OffBroadway.MQTT.Config.new(
      ...>   telemetry_prefix: :test,
      ...>     server_opts: [host: "vernemq", port: 8883, transport: :ssl]
      ...>   )
      %OffBroadway.MQTT.Config{
        acknowledger: OffBroadway.MQTT.Acknowledger,
        client: OffBroadway.MQTT.Client,
        client_id_prefix: "obmp",
        dequeue_interval: 5000,
        handler: OffBroadway.MQTT.Handler,
        producer: OffBroadway.MQTT.Producer,
        queue: OffBroadway.MQTT.Queue,
        queue_registry: OffBroadway.MQTT.QueueRegistry,
        queue_supervisor: OffBroadway.MQTT.QueueSupervisor,
        server: {:ssl, [host: 'vernemq', port: 8883]},
        telemetry_prefix: :test
      }

  Keep in mind that any option with nil or the empty string will be removed from
  the server options to prevent issues when configuring the application from
  environment variables.
  """

  alias OffBroadway.MQTT.Acknowledger
  alias OffBroadway.MQTT.Client
  alias OffBroadway.MQTT.Handler
  alias OffBroadway.MQTT.Producer
  alias OffBroadway.MQTT.Queue

  @type transport :: :tcp | :ssl

  @type raw_server_opt ::
          {:host, String.t()}
          | {:port, non_neg_integer | String.t()}
          | {:transport, transport, String.t()}
          | {atom, any}

  @type raw_server_opts :: [raw_server_opt, ...]

  @type server_opt ::
          {:host, String.t()}
          | {:port, non_neg_integer}
          | {:transport, transport}
          | {atom, any}
  @type server_opts :: [server_opt, ...]

  @type server :: {transport, server_opts}

  @type option ::
          {:acknowledger, module}
          | {:client, module}
          | {:client_id_prefix, String.t()}
          | {:dequeue_interval, non_neg_integer}
          | {:handler, module}
          | {:producer, module}
          | {:queue, module}
          | {:queue_registry, GenServer.name()}
          | {:queue_supervisor, GenServer.name()}
          | {:server_opts, raw_server_opts}
          | {:telemetry_prefix, atom}
          | {atom, any}

  @type options :: [option]

  @type t :: %__MODULE__{
          acknowledger: module,
          client: module,
          client_id_prefix: String.t(),
          dequeue_interval: non_neg_integer,
          handler: module,
          producer: module,
          queue: module,
          queue_registry: GenServer.name(),
          queue_supervisor: GenServer.name(),
          server: server,
          telemetry_prefix: atom
        }

  defstruct [
    :acknowledger,
    :client,
    :client_id_prefix,
    :dequeue_interval,
    :handler,
    :producer,
    :queue,
    :queue_supervisor,
    :queue_registry,
    :server,
    :telemetry_prefix
  ]

  @doc """
  Returns a `t:t/0` with values from the applicatoin config.

  Use this function if you want to build a config based on the configured
  defaults for the application.
  """
  @spec new_from_app_config(options) :: t
  def new_from_app_config(overrides \\ []) do
    :off_broadway_mqtt
    |> Application.get_all_env()
    |> Keyword.merge(overrides)
    |> new()
  end

  @doc """
  Builds a `t:t/0` from the given keyword list.

  Use this function if you want to build a config only from the passed options.
  Any application config is ignored if using this function.
  """
  @spec new(options) :: t
  def new(opts \\ []) when is_list(opts) do
    {transport, server_opts} =
      opts
      |> Keyword.get(:server_opts, [])
      |> Keyword.update(:host, @default_host, &parse_host/1)
      |> Keyword.update(:port, @default_port, &parse_port/1)
      |> Keyword.update(:transport, @default_transport, &parse_transport/1)
      |> Keyword.pop(:transport, @default_transport)

    server_opts = Enum.reject(server_opts, &is_empty?/1)

    struct_opts =
      opts
      |> Keyword.put_new(:acknowledger, Acknowledger)
      |> Keyword.put_new(:client, Client)
      |> Keyword.put_new(:client_id_prefix, @default_client_id_prefix)
      |> Keyword.put_new(:dequeue_interval, @default_dequeue_interval)
      |> Keyword.put_new(:handler, Handler)
      |> Keyword.put_new(:producer, Producer)
      |> Keyword.put_new(:queue, Queue)
      |> Keyword.put_new(:queue_registry, @default_registry)
      |> Keyword.put_new(:queue_supervisor, @default_supervisor)
      |> Keyword.put_new(:server, {transport, server_opts})
      |> Keyword.put_new(:telemetry_prefix, @default_telemetry_prefix)

    struct(__MODULE__, struct_opts)
  end

  defp is_empty?({_, ""}), do: true
  defp is_empty?({_, nil}), do: true
  defp is_empty?(_), do: false

  defp parse_host(host) when is_binary(host), do: host

  defp parse_port(port) when is_integer(port), do: port

  defp parse_port(port) when is_binary(port) do
    case Integer.parse(port) do
      {port, ""} ->
        port

      _ ->
        raise ArgumentError,
              "invalid port configured for #{__MODULE__}: \"#{port}\""
    end
  end

  defp parse_port(port) do
    raise ArgumentError,
          "invalid port configured for #{__MODULE__}: \"#{port}\""
  end

  defp parse_transport(t) when t in ["TCP", "tcp", :tcp], do: :tcp
  defp parse_transport(t) when t in ["SSL", "ssl", :ssl], do: :ssl

  defp parse_transport(t) do
    raise ArgumentError,
          "invalid transport configured for #{__MODULE__}: \"#{t}\".\n" <>
            "Allowed values are `TCP` and `SSL`."
  end
end
