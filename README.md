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

- `off_broadway_mqtt_producer.client.connection.up.count`
- `off_broadway_mqtt_producer.client.connection.down.count`
- `off_broadway_mqtt_producer.client.subscription.up.count`
- `off_broadway_mqtt_producer.client.subscription.down.count`
- `off_broadway_mqtt_producer.client.messages.count`
- `off_broadway_mqtt_producer.queue.in.count`
- `off_broadway_mqtt_producer.queue.out.count`
- `off_broadway_mqtt_producer.acknowledger.success.count`
- `off_broadway_mqtt_producer.acknowledger.failed.count`
- `off_broadway_mqtt_producer.acknowledger.ignored.count`
- `off_broadway_mqtt_producer.acknowledger.requeued.count`

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
