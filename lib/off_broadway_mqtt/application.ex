defmodule OffBroadway.MQTT.Application do
  @moduledoc false

  use Application

  def start(_type, _args) do
    children = [
      {Registry, [keys: :unique, name: OffBroadway.MQTT.QueueRegistry]},
      {DynamicSupervisor,
       [strategy: :one_for_one, name: OffBroadway.MQTT.QueueSupervisor]}
    ]

    opts = [strategy: :one_for_one, name: OffBroadway.MQTT.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
