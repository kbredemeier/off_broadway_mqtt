defmodule OffBroadwayTortoise.Application do
  @moduledoc false

  use Application

  def start(_type, _args) do
    children = [
      {Registry, [keys: :unique, name: OffBroadwayTortoise.QueueRegistry]},
      {DynamicSupervisor,
       [strategy: :one_for_one, name: OffBroadwayTortoise.QueueSupervisor]}
    ]

    opts = [strategy: :one_for_one, name: OffBroadwayTortoise.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
