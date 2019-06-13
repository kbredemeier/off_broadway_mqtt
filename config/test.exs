use Mix.Config

config :off_broadway_tortoise,
  client_id_prefix: "off_broadway_tortoise",
  connection: [
    host: "vernemq",
    port: 1883
  ]
