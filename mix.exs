defmodule OffBroadway.MQTT.MixProject do
  use Mix.Project

  def project do
    [
      app: :off_broadway_mqtt,
      deps: deps(),
      docs: [main: "readme", extras: ["README.md"]],
      description: "A MQTT connector for Broadway",
      elixir: "~> 1.10",
      elixirc_paths: elixirc_paths(Mix.env()),
      package: package(),
      start_permanent: Mix.env() == :prod,
      version: "0.2.0",

      # Coveralls
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test,
        "coveralls.circle": :test
      ],
      source_url: "https://github.com/kbredemeier/off_broadway_mqtt",

      # Dialyzer
      dialyzer: [
        flags: [
          :underspecs,
          :unknown
        ]
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  def application do
    [
      extra_applications: [:logger],
      mod: {OffBroadway.MQTT.Application, []}
    ]
  end

  defp package do
    [
      name: "off_broadway_mqtt_connector",
      maintainers: ["Kristopher Bredemeier"],
      licenses: ["Apache-2.0"],
      files: ["lib", "mix.exs", "README*", "CHANGELOG*", "LICENSE*"],
      links: %{
        "GitHub" => "https://github.com/kbredemeier/off_broadway_mqtt"
      }
    ]
  end

  defp deps do
    [
      {:broadway, "~> 0.6.2"},
      {:credo, "~> 1.0", only: [:dev, :test]},
      {:dialyxir, "~> 1.0.0-rc.6", only: [:dev], runtime: false},
      {:ex_doc, "~> 0.20", only: :dev, runtime: false},
      {:excoveralls, "~> 0.10", only: :test},
      {:telemetry, "~> 0.4.0"},
      {:tortoise, "~> 0.9"}
    ]
  end
end
