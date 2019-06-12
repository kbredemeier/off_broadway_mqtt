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
  Starts a `#{Queue}` and puts it's registered name under `queue` to the
  context.
  """
  def start_queue(%{test: test_name} = context) do
    name =
      case Map.get(context, :start_queue) do
        true -> :"#{test_name} queue"
        name -> name
      end

    {:ok, pid} = start_supervised({Queue, name})

    context
    |> Map.put(:queue, name)
    |> Map.put(:pid, pid)
  end
end
