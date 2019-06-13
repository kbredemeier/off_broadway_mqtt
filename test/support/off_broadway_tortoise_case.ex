defmodule OffBroadway.MQTTProducerCase do
  @moduledoc """
  Test templace for testing aspects of this library.

  Provides utility to start all the necessary dependencies.
  """

  use ExUnit.CaseTemplate

  alias OffBroadway.MQTTProducer.Queue

  using _opts do
    quote do
      import OffBroadway.MQTTProducerCase
      import OffBroadway.MQTTProducer.Factory
      alias OffBroadway.MQTTProducer.Data
      alias OffBroadway.MQTTProducer.Queue
    end
  end

  setup tags do
    tags
    |> start_registry_tag
    |> start_supervisor_tag
    |> start_mqtt_client_tag
    |> start_queue_tag
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
    client_id = OffBroadway.MQTTProducer.unique_client_id()
    subscriptions = Map.get(context, :subscriptions) || []

    %{conn: {_, mqtt_opts}} = OffBroadway.MQTTProducer.config()

    tortoise_opts = [
      client_id: client_id,
      handler: {OffBroadway.MQTTProducer.TestHandler, [pid: self()]},
      server: {Tortoise.Transport.Tcp, mqtt_opts},
      subscriptions: subscriptions
    ]

    {:ok, _} = Tortoise.Connection.start_link(tortoise_opts)

    receive do
      {:test_mqtt_client, :up} -> :ok
    after
      5000 ->
        raise "test mqtt client connection timed out: #{inspect(mqtt_opts)}"
    end

    context
    |> Map.put(:test_client_id, client_id)
  end

  @doc """
  Starts a `#{inspect(Queue)}` and puts it's registered name under `queue` to the
  context.
  """
  def start_queue(%{test: test_name} = context) do
    registry = Map.get(context, :registry, OffBroadway.MQTTProducer.QueueRegistry)

    queue_name =
      context
      |> Map.get(:start_queue, test_name)
      |> case do
        {:via, _, _} = reg_name -> reg_name
        true -> OffBroadway.MQTTProducer.queue_name(registry, to_string(test_name))
        name -> OffBroadway.MQTTProducer.queue_name(registry, name)
      end

    {:via, _, {_, topic}} = queue_name

    {:ok, pid} = start_supervised({Queue, queue_name})

    context
    |> Map.put(:queue, queue_name)
    |> Map.put(:queue_topic, topic)
    |> Map.put(:pid, pid)
  end

  @doc """
  Returns options to start `OffBroadway.MQTTProducer.TestBroadway` with.
  """
  def test_broadway_opts_from_context(context, overrides \\ []) do
    producer_opts =
      context
      |> Map.take([:registry, :supervisor])
      |> Enum.into([])
      |> Keyword.put_new(:dequeue_interval, 100)

    [
      name: :"#{context.test}_broadway",
      topic: "#{context.test}_topic",
      producer_opts: producer_opts
    ]
    |> Keyword.merge(overrides)
  end
end
