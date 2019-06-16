defmodule OffBroadway.MQTTProducer.ConfigTest do
  use ExUnit.Case, async: true

  alias OffBroadway.MQTTProducer.Config

  @config_opts ~w(
    acknowledger
    client
    client_id_prefix
    dequeue_interval
    handler
    queue
    queue_registry
    queue_supervisor
    telemetry_prefix
  )a

  describe "new/2" do
    test "defaults" do
      config = Config.new()
      assert Code.ensure_compiled?(config.acknowledger)
      assert Code.ensure_compiled?(config.client)
      assert Code.ensure_compiled?(config.handler)
      assert Code.ensure_compiled?(config.queue)
      assert config.client_id_prefix == "off_broadway_mqtt_producer"
      assert config.dequeue_interval == 100
      assert config.queue_registry == OffBroadway.MQTTProducer.QueueRegistry
      assert config.queue_supervisor == OffBroadway.MQTTProducer.QueueSupervisor
      assert config.telemetry_prefix == :off_broadway_mqtt_producer
    end

    test "reads config from Application environment" do
      assert config_opts = Application.get_all_env(:off_broadway_mqtt_producer)
      refute Enum.empty?(config_opts)
      assert Config.new(config_opts) == Config.new()
      assert Config.new(config_opts) == Config.new(:default)
      assert Config.new(config_opts) == Config.new(:default, config_opts)
      assert Config.new(config_opts) == Config.new(:default, [])
    end

    test "adds additional opts to server" do
      assert %{server: {_, applied_opts}} =
               Config.new(server_opts: [password: "foo"])

      assert {:password, "foo"} in applied_opts

      assert %{server: {_, applied_opts}} =
               Config.new(:default, server_opts: [password: "foo"])

      assert {:password, "foo"} in applied_opts
    end

    for key <- @config_opts do
      test "takes #{key} from config_opts" do
        assert %{unquote(key) => __MODULE__} =
                 Config.new([{unquote(key), __MODULE__}])
      end
    end

    test "sets host from a charlist" do
      assert %{server: {_, [host: 'test_host', port: _]}} =
               Config.new(server_opts: [host: 'test_host'])
    end

    test "sets host from a string" do
      assert %{server: {_, [host: 'test_host', port: _]}} =
               Config.new(server_opts: [host: "test_host"])
    end

    test "sets port from integer" do
      assert %{server: {_, [port: 1111, host: _]}} =
               Config.new(server_opts: [port: 1111])
    end

    test "sets port from string" do
      assert %{server: {_, [port: 1111, host: _]}} =
               Config.new(server_opts: [port: "1111"])
    end

    for value <- ["aaa", 1.2, :foo] do
      test "raises error if invalid port #{inspect(value)} is given" do
        assert_raise ArgumentError, fn ->
          Config.new(server_opts: [port: unquote(value)])
        end
      end
    end

    test "sets transport from server_opts" do
      assert %{server: {:ssl, _}} = Config.new(server_opts: [transport: :ssl])
      assert %{server: {:ssl, _}} = Config.new(server_opts: [transport: "ssl"])
      assert %{server: {:ssl, _}} = Config.new(server_opts: [transport: "SSL"])
      assert %{server: {:tcp, _}} = Config.new(server_opts: [transport: :tcp])
      assert %{server: {:tcp, _}} = Config.new(server_opts: [transport: "tcp"])
      assert %{server: {:tcp, _}} = Config.new(server_opts: [transport: "TCP"])
    end

    for value <- [123, "asd", :foo] do
      test "raises error if invalid transport #{inspect(value)} is given" do
        assert_raise ArgumentError, fn ->
          Config.new(server_opts: [transport: unquote(value)])
        end
      end
    end
  end
end
