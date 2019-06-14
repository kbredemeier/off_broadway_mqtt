use Mix.Config

config :off_broadway_mqtt_producer,
  client_id_prefix: "off_broadway_mqtt_producer",
  server_opts: [
    host: "vernemq",
    port: 1883
  ]
