# OffBroadway.MQTTProducer

A MQTT connector for [Broadway](https://github.com/plataformatec/broadway).

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `off_broadway_mqtt_producer` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:off_broadway_mqtt_producer, "~> 0.1.0"}
  ]
end
```

## Usage

```elixir
defmodule OffBroadway.MQTTProducer.TestBroadway do
  use OffBroadway.MQTTProducer

  def start_link(config, topic) do
    Broadway.start_link(__MODULE__,
      name: name,
      producers: [
        default: [
          module: {Producer, [config, subscription: {topic, 0}]},
          stages: 1
        ]
      ],
      processors: [default: [stages: 1]],
      batchers: [
        default: [stages: 1, batch_size: 10]
      ]
    )
  end

  @impl true
  def handle_message(_processor_name, message, _context) do
    handle_errors(message) do
      updated_data = String.upcase(message.data)

      if updated_data == "NINCOMPOOP" do
        raise OffBroadway.MQTTProducer.Error,
          message: "that was foolish", ack: :retry
      end

      %{message | data: updated_data}
    end
  end
end
```

## Telemetry events

Telemetry events are disabled by default. To enable them the following must be
configured at compile time:

```elxir
use Mix.Config

config :off_broadway_mqtt_producer,
  telemetry_enabled: true,
```

A prefix can be configured that is used to prefix any telemetry event.

```elxir
use Mix.Config

config :off_broadway_mqtt_producer,
  telemetry_prefix: :my_app,
```

The prefix can also be passed at runtime with the
`t:OffBroadway.MQTTProducer.Config.t/0` to the producer.

The following events are emitted:

- `my_app.client.connection.up.count`
- `my_app.client.connection.down.count`
- `my_app.client.subscription.up.count`
- `my_app.client.subscription.down.count`
- `my_app.client.messages.count`
- `my_app.queue.in.count`
- `my_app.queue.in.size`
- `my_app.queue.out.count`
- `my_app.queue.out.size`
- `my_app.acknowledger.success.count`
- `my_app.acknowledger.failed.count`
- `my_app.acknowledger.ignored.count`
- `my_app.acknowledger.requeued.count`

## License

Copyright 2019 Kristopher Bredemeier

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
