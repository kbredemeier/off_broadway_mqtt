[![Hex version badge](https://img.shields.io/hexpm/v/off_broadway_mqtt_connector.svg)](https://hex.pm/packages/off_broadway_mqtt_connector)
[![Coverage Status](https://coveralls.io/repos/github/kbredemeier/off_broadway_mqtt/badge.svg?branch=master)](https://coveralls.io/github/kbredemeier/off_broadway_mqtt?branch=master)
[![CircleCI](https://circleci.com/gh/kbredemeier/off_broadway_mqtt.svg?style=svg)](https://circleci.com/gh/kbredemeier/off_broadway_mqtt)
[![GitHub license](https://img.shields.io/github/license/kbredemeier/off_broadway_mqtt.svg)](https://github.com/kbredemeier/off_broadway_mqtt/blob/master/LICENSE)

# OffBroadway.MQTT

A MQTT connector for [Broadway](https://github.com/plataformatec/broadway).

## Installation

Add `off_broadway_mqtt_connector` to the list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:off_broadway_mqtt, "~> 0.1.0", hex: "off_broadway_mqtt_connector"}
  ]
end
```

Notice that the package has a different name than the application!

## Usage

Add it as a producer to your `Broadway`:

```elixir
defmodule MyApp.NincompoopFilter do
  use OffBroadway.MQTT

  defmodule Nincompoop do
    defexception ack: :ignore, message: nil

    def message(e) do
      "message is probably coming from a nincompoop: " <> e.message
    end
  end

  def start_link(config, topic) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
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
    message
    |> Message.update_data(&process_data/1)
  rescue
    e ->
      Message.failed(message, e)
  end

  defp process_data(%OffBroadway.MQTT.Data{acc: msg} = data) do
    msg
    |> String.downcase()
    |> String.contains?("great again")
    |> case do
      true -> raise Nincompoop, "contains \"great again\""
      false -> data
    end
  end

  @impl true
  def handle_batch(_, messages, _batch_info, _context) do
    # ...
    messages
  end
end
```

Start it by passing it at least a `t:OffBroadway.MQTT.Config.t/0` and the
`subscription` option with a topic to subscribe to and the desired QOS. For
further options refer to the `OffBroadway.MQTT.Producer` docs.

Default values for the configuration can be given via the mix config:

```elixir
use Mix.Config

config :off_broadway_mqtt,
  client_id_prefix: "sensor_data_processor",
  server_opts: [
    host: "vernemq",
    port: 8883,
    transport: :ssl
  ],
  handler: MyApp.BetterHandler
```

Then build a config and start your broadway:

```elixir
# Builds a configuration with all configured default values
config = OffBroadway.MQTT.Config.new()

# Builds a configuration from the defaults and overrides values
config =
  OffBroadway.MQTT.Config.new(
    client_id_prefix: "myapp",
    server_opts: [
      # host is converted into a `charlist`
      host: "mosquitto",
      # port is converted into a `integer` if it is not already one
      port: "1883",
      transport: :tcp,
      username: "admin",
      password: "admin"
    ]
  )

# Start broadway
MyApp.NincompoopFilter.start(config, "test_topic")
```

## Telemetry events

Telemetry events are disabled by default. To enable them the following must be
configured at compile time:

```elixir
use Mix.Config

config :off_broadway_mqtt,
  telemetry_enabled: true,
```

A prefix can be configured that is used to prefix any telemetry event.

```elixir
use Mix.Config

config :off_broadway_mqtt,
  telemetry_prefix: :my_broadway,
```

The prefix can also be passed at runtime with the
`t:OffBroadway.MQTT.Config.t/0` to the producer.

The following events are emitted:

- `my_broadway.client.connection.up.count`
- `my_broadway.client.connection.down.count`
- `my_broadway.client.subscription.up.count`
- `my_broadway.client.subscription.down.count`
- `my_broadway.client.messages.count`
- `my_broadway.queue.in.count`
- `my_broadway.queue.in.size`
- `my_broadway.queue.out.count`
- `my_broadway.queue.out.size`
- `my_broadway.acknowledger.success.count`
- `my_broadway.acknowledger.failed.count`
- `my_broadway.acknowledger.ignored.count`
- `my_broadway.acknowledger.requeued.count`

## Development

For development you probably need a running MQTT server in your develoment
environment. The provided `docker-compose.yml` starts a `vernemq` container for you.

Then run the following commands:

```
mix deps.get
mix test
```

The documentation can be generated with `mix docs`. Code coverage report can be
generated with `mix coveralls.html`.

## License

Copyright 2019 Kristopher Bredemeier

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

[http://www.apache.org/licenses/LICENSE-2.0](http://www.apache.org/licenses/LICENSE-2.0)

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
