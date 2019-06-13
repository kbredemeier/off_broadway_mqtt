defmodule OffBroadway.MQTTProducer.Application do
  @moduledoc false

  use Application

  def start(_type, _args) do
    children = [
      {Registry, [keys: :unique, name: OffBroadway.MQTTProducer.QueueRegistry]},
      {DynamicSupervisor,
       [strategy: :one_for_one, name: OffBroadway.MQTTProducer.QueueSupervisor]}
    ]

    opts = [strategy: :one_for_one, name: OffBroadway.MQTTProducer.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
