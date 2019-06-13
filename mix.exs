defmodule OffBroadwayTortoise.MixProject do
  use Mix.Project

  def project do
    [
      app: :off_broadway_tortoise,
      deps: deps(),
      docs: [extras: ["README.md"]],
      elixir: "~> 1.8",
      elixirc_paths: elixirc_paths(Mix.env()),
      package: package(),
      start_permanent: Mix.env() == :prod,
      version: "0.1.0",

      # Coveralls
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [
        coveralls: :test,
        "coveralls.detail": :test,
        "coveralls.post": :test,
        "coveralls.html": :test
      ]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {OffBroadwayTortoise.Application, []}
    ]
  end

  defp package do
    [
      maintainers: ["Kristopher Bredemeier"],
      licenses: ["Apache 2.0"],
      files: ["lib", "mix.exs", "README*", "CHANGELOG*", "LICENSE*"],
      links: %{
        "GitHub" => "https://github.com/kbredemeier/off_broadway_tortoise"
      }
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:gen_stage, "~> 0.14"},
      {:tortoise, "~> 0.9"},
      {:broadway, "~> 0.3.0"},
      {:credo, "~> 1.0", only: [:dev, :test]},
      {:ex_doc, "~> 0.19.2", only: :dev, runtime: false},
      {:excoveralls, "~> 0.10", only: :test}
    ]
  end
end
