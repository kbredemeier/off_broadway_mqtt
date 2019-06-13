defmodule OffBroadwayTortoise.QueueCase do
  @moduledoc """
  Utility for testing testing `OffBroadwayTortoise.Queue` related things.
  """

  use ExUnit.CaseTemplate

  alias OffBroadwayTortoise
  alias OffBroadwayTortoise.Queue

  using _opts do
    quote do
      import OffBroadwayTortoise.QueueCase
      alias OffBroadwayTortoise.Queue
    end
  end

  setup tags do
    tags
    |> start_queue_tag
  end

  defp start_queue_tag(tags) do
    if tags[:start_queue],
      do: start_queue(tags),
      else: tags
  end

  @doc """
  Starts a `#{inspect(Queue)}` and puts it's registered name under `queue` to the
  context.
  """
  def start_queue(%{test: test_name} = context) do
    registry = Map.get(context, :registry, OffBroadwayTortoise.QueueRegistry)

    queue_name =
      context
      |> Map.get(:start_queue, test_name)
      |> case do
        {:via, _, _} = reg_name -> reg_name
        true -> OffBroadwayTortoise.queue_name(registry, to_string(test_name))
        name -> OffBroadwayTortoise.queue_name(registry, name)
      end

    {:via, _, {_, topic}} = queue_name

    {:ok, pid} = start_supervised({Queue, queue_name})

    context
    |> Map.put(:queue, queue_name)
    |> Map.put(:queue_topic, topic)
    |> Map.put(:pid, pid)
  end
end
