defmodule OffBroadway.MQTT.Assertions do
  @moduledoc """
  Provices test assertion helpers to test `OffBroadway.MQTT`.
  """

  @doc """
  Tests that there is a `Tortoise.Connection` for the given `client_id`
  registered with the `Tortoise.Registry`.

  Note that this does not necessarily mean that the client is connected!
  """
  def assert_mqtt_client_running(client_id, tries \\ 0)

  def assert_mqtt_client_running(client_id, 10) do
    raise ExUnit.AssertionError,
      message:
        "Expected a Tortoise.Connection running for " <>
          "#{inspect(client_id)} but there isn't any."
  end

  def assert_mqtt_client_running(client_id, tries) do
    Tortoise.Registry
    |> Registry.lookup({Tortoise.Connection, client_id})
    |> case do
      [] -> assert_mqtt_client_running(client_id, tries + 1)
      [_] -> true
    end
  end

  @doc """
  Tests that there is no `Tortoise.Connection` for the given `client_id`
  registered with the `Tortoise.Registry`.
  """
  def refute_mqtt_client_running(client_id, tries \\ 0)

  def refute_mqtt_client_running(client_id, 5) do
    raise ExUnit.AssertionError,
      message:
        "Expected NO Tortoise.Connection running for " <>
          "#{inspect(client_id)} but there is one."
  end

  def refute_mqtt_client_running(client_id, tries) do
    Tortoise.Registry
    |> Registry.lookup({Tortoise.Connection, client_id})
    |> case do
      [_] ->
        Process.sleep(500)
        refute_mqtt_client_running(client_id, tries + 1)

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

  @doc false
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
