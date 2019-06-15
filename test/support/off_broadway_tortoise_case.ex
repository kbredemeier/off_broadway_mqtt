defmodule OffBroadway.MQTTProducerCase do
  @moduledoc """
  Test templace for testing aspects of this library.

  Provides utility to start all the necessary dependencies.
  """

  use ExUnit.CaseTemplate

  alias OffBroadway.MQTTProducer.Queue
  alias OffBroadway.MQTTProducer.Config

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

    server_opts =
      :off_broadway_mqtt_producer
      |> Application.get_all_env()
      |> Keyword.get(:server_opts, [])
      |> Keyword.drop([:protocol])

    tortoise_opts = [
      client_id: client_id,
      handler: {OffBroadway.MQTTProducer.TestHandler, [pid: self()]},
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
    config = config_from_context(context)

    queue_name =
      context
      |> Map.get(:start_queue, test_name)
      |> case do
        {:via, _, _} = reg_name ->
          reg_name

        true ->
          OffBroadway.MQTTProducer.queue_name(config, to_string(test_name))

        name ->
          OffBroadway.MQTTProducer.queue_name(config, name)
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

  def config_from_context(%{registry: reg, supervisor: sup}) do
    Config.new(:default, queue_registry: reg, queue_supervisor: sup)
  end

  @doc """
  Tests that there is a `Tortoise.Connection` for the given `client_id`
  registered with the `Tortoise.Registry`.

  Note that this does not necessarily mean that the client is connected!
  """
  def assert_mqtt_client_running(client_id) do
    Tortoise.Registry
    |> Registry.lookup({Tortoise.Connection, client_id})
    |> case do
      [] ->
        raise ExUnit.AssertionError,
          message:
            "Expected a Tortoise.Connection running for " <>
              "#{inspect(client_id)} but there isn't any."

      [_] ->
        true
    end
  end

  @doc """
  Tests that there is no `Tortoise.Connection` for the given `client_id`
  registered with the `Tortoise.Registry`.
  """
  def refute_mqtt_client_running(client_id) do
    Tortoise.Registry
    |> Registry.lookup({Tortoise.Connection, client_id})
    |> case do
      [_] ->
        raise ExUnit.AssertionError,
          message:
            "Expected NO Tortoise.Connection running for " <>
              "#{inspect(client_id)} but there is one."

      [] ->
        true
    end
  end

  @doc """
  When sarting the mqtt client or producer you can pass `sub_ack: self()` with
  the options to receive a message from the handler as soon as the subscription
  is confirmed. The message received has the form
  `{:sunscription, client_id, topic, status}`.
  """
  defmacro assert_receive_sub_ack(client_id, topic) do
    do_assert_receive_sub_ack(client_id, topic)
  end

  defp do_assert_receive_sub_ack(client_id, topic) do
    client_id_expr = maybe_pin(client_id)
    topic_expr = maybe_pin(topic)

    client_id_str = inspect_arg(client_id)
    topic_str = inspect_arg(topic)

    quote do
      receive do
        {:subscription, unquote(client_id_expr), unquote(topic_expr), :up} ->
          true
      after
        2000 ->
          acks =
            collect_messages()
            |> Enum.filter(fn
              {:subscription, _, _, _} -> true
              _ -> false
            end)
            |> Enum.map(fn {_, client_id, topic, status} ->
              "- #{inspect(status)} for #{inspect(client_id)} on " <>
                " #{inspect(topic)}"
            end)
            |> case do
              [] -> "none"
              acks -> Enum.join(acks, "\n")
            end

          raise ExUnit.AssertionError,
            message:
              "Expected the Tortoise Handler to acknowledge it's " <>
                "subscription for #{unquote(client_id_str)} client_id to " <>
                "#{unquote(topic_str)} topic but it didn't.\n\n" <>
                "The following subsriptions have been acknowledged:\n\n" <>
                "#{acks}\n"
      end
    end
  end

  def collect_messages(acc \\ [], action \\ :cont)
  def collect_messages(acc, :halt), do: acc

  def collect_messages(acc, :cont) do
    receive do
      msg ->
        collect_messages([msg | acc], :cont)
    after
      100 -> collect_messages(acc, :halt)
    end
  end

  defp maybe_pin({:_, _, _} = expr), do: expr

  defp maybe_pin({_, _, _} = expr) do
    quote do
      ^unquote(expr)
    end
  end

  defp maybe_pin(expr), do: expr

  defp inspect_arg({:_, _, _}), do: "any"

  defp inspect_arg({_, _, _} = arg) do
    quote do
      inspect(unquote(arg))
    end
  end

  defp inspect_arg(arg), do: inspect(arg)
end
