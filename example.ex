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
    IO.puts("handle")

    message
    |> Message.update_data(&process_data/1)
  rescue
    e ->
      Message.failed(message, e)
  end

  defp process_data(%OffBroadway.MQTT.Data{acc: msg} = data) do
    IO.puts("processing")

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

# Builds a configuration from the default and overrides certail values
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

# Builds a configuration with all configured default values
config = OffBroadway.MQTT.Config.new()

# Start broadway
{:ok, pid} = MyApp.NincompoopFilter.start_link(config, "test_topic")
