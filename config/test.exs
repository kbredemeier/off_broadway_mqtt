use Mix.Config

config :off_broadway_mqtt_producer,
  client_id_prefix: "off_broadway_mqtt_producer",
  connection: [
    host: "vernemq",
    port: 1883
  ]
