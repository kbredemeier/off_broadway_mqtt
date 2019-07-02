defmodule OffBroadway.MQTTCase do
  @moduledoc """
  Test templace for testing aspects of this library.

  Provides utility to start all the necessary dependencies.
  """

  use ExUnit.CaseTemplate

  alias OffBroadway.MQTT.Queue
  alias OffBroadway.MQTT.Config

  using _opts do
    quote do
      import OffBroadway.MQTT.Assertions
      import OffBroadway.MQTT.Factory
      import OffBroadway.MQTTCase

      alias OffBroadway.MQTT
      alias OffBroadway.MQTT.Data
      alias OffBroadway.MQTT.Queue
    end
  end

  setup tags do
    tags
    |> subscribe_telemetry_event_tag
    |> start_registry_tag
    |> start_supervisor_tag
    |> start_mqtt_client_tag
    |> build_config_tag
    |> start_queue_tag
  end

  defp subscribe_telemetry_event_tag(tags) do
    if tags[:subscribe_telemetry_event],
      do: subscribe_telemetry_event(tags),
      else: tags
  end

  defp build_config_tag(tags) do
    if tags[:build_config],
      do: build_config(tags),
      else: tags
  end

  defp start_registry_tag(tags) do
    if tags[:start_registry],
      do: start_registry(tags),
      else: tags
  end

  defp start_supervisor_tag(tags) do
    if tags[:start_supervisor],
      do: start_supervisor(tags),
      else: tags
  end

  defp start_mqtt_client_tag(tags) do
    if tags[:start_mqtt_client],
      do: start_mqtt_client(tags),
      else: tags
  end

  defp start_queue_tag(tags) do
    if tags[:start_queue],
      do: start_queue(tags),
      else: tags
  end

  @doc """
  Starts a `Registry` and puts it's registered name under `registry`
  to the context.
  """
  def start_registry(%{test: name_prefix} = context) do
    name = :"#{name_prefix} registry"

    {:ok, _} = start_supervised({Registry, [name: name, keys: :unique]})

    context
    |> Map.put(:registry, name)
  end

  @doc """
  Starts a `Registry` and puts it's registered name under `supervisor`
  to the context.
  """
  def start_supervisor(%{test: name_prefix} = context) do
    name = :"#{name_prefix} supervisor"

    {:ok, _} =
      start_supervised(
        {DynamicSupervisor, [name: name, strategy: :one_for_one]}
      )

    context
    |> Map.put(:supervisor, name)
  end

  @doc """
  Starts a `Tortoise.Connection` and puts it's client_id under `test_client_id`
  to the context.
  """
  def start_mqtt_client(context) do
    {client_id, subscriptions} =
      context
      |> Map.get(:start_mqtt_client, [])
      |> case do
        [] -> {build_test_client_id(), []}
        {client_id, subscriptions} -> {client_id, subscriptions}
        client_id -> {client_id, []}
      end

    server_opts =
      :off_broadway_mqtt
      |> Application.get_all_env()
      |> Keyword.get(:server_opts, [])
      |> Keyword.drop([:protocol])

    tortoise_opts = [
      client_id: client_id,
      handler: {OffBroadway.MQTT.TestHandler, [pid: self()]},
      server: {Tortoise.Transport.Tcp, server_opts},
      subscriptions: subscriptions
    ]

    {:ok, _} = Tortoise.Connection.start_link(tortoise_opts)

    receive do
      {:test_mqtt_client, :up} -> :ok
    after
      5000 ->
        raise "test mqtt client connection timed out: #{inspect(server_opts)}"
    end

    context
    |> Map.put(:test_client_id, client_id)
  end

  @doc """
  Starts a `#{inspect(Queue)}` and puts it's registered name under `queue` to the
  context.
  """
  def start_queue(%{test: test_name} = context) do
    config = context[:config] || config_from_context(context)

    queue_name =
      context
      |> Map.get(:start_queue, test_name)
      |> case do
        {:via, _, _} = reg_name ->
          reg_name

        true ->
          OffBroadway.MQTT.queue_name(config, to_string(test_name))

        name ->
          OffBroadway.MQTT.queue_name(config, name)
      end

    {:via, _, {_, topic}} = queue_name

    {:ok, pid} = start_supervised({Queue, [config, queue_name]})

    context
    |> Map.put(:queue, queue_name)
    |> Map.put(:queue_topic, topic)
    |> Map.put(:pid, pid)
  end

  @doc """
  Builds a random but unique client id.
  """
  def build_test_client_id do
    "test_client_#{System.unique_integer([:positive])}"
  end

  @doc """
  Builds a configuration from the context.

  It adds the supervisor and registry if avalilable.
  """
  def config_from_context(%{registry: reg, supervisor: sup}) do
    Config.new(:default,
      queue_registry: reg,
      queue_supervisor: sup
    )
  end

  def config_from_context(_) do
    Config.new(:default)
  end

  @doc """
  Subscribes to the event given with `:subscribe_telemetry_event` and forwards
  them to the test process.
  """
  def subscribe_telemetry_event(context) do
    event =
      case context[:subscribe_telemetry_event] do
        [_ | _] = event -> event
        _ -> raise "no telemetry event given to subscribe!"
      end

    test_pid = self()

    handle_event = fn event, topic, data, extra ->
      send(test_pid, {:telemetry, event, topic, data, extra})
    end

    context.test
    |> to_string
    |> :telemetry.attach(event, handle_event, nil)

    context
    |> Map.put(:event, event)
  end

  @doc """
  Builds a config from the values in the context and adds it under the `:config`
  key to the context.
  """
  def build_config(context) do
    Map.put(context, :config, config_from_context(context))
  end
end
