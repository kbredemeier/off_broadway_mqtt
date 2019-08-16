defmodule OffBroadway.MQTTTest do
  use OffBroadway.MQTTCase, async: true

  alias OffBroadway.MQTT.Config

  @moduletag capture_log: true

  describe "unique_client_id_/1" do
    test "returns a string" do
      assert id = MQTT.unique_client_id()
      assert is_binary(id)
      assert Regex.match?(~r/^test_.+/, id)
    end

    test "returns unique client ids" do
      ids =
        for _ <- 1..100 do
          MQTT.unique_client_id()
        end

      assert ids == Enum.uniq(ids)
    end

    test "takes the client_id_prefix from config" do
      assert id =
               MQTT.unique_client_id(%{
                 client_id_prefix: "asd"
               })

      assert Regex.match?(~r/^asd_.+/, id)
    end
  end

  test "topic_from_queue_name/1" do
    assert "test" = MQTT.topic_from_queue_name({:via, Registry, {:foo, "test"}})
  end

  describe "queue_name" do
    test "returns the process name for a topic queue server" do
      assert {:via, Registry, {:foo, "bar"}} ==
               MQTT.queue_name(
                 %{queue_registry: :foo},
                 "bar"
               )
    end

    test "returns the process name with the default registry" do
      assert {:via, Registry, {MQTT.QueueRegistry, "bar"}} ==
               MQTT.queue_name("bar")
    end
  end

  describe "default_config/1" do
    test "returns the mqtt config" do
      assert %Config{
               client_id_prefix: "test",
               server: {:tcp, [host: "localhost", port: 1883]}
             } = MQTT.default_config()
    end

    test "sets transport" do
      assert %Config{
               server: {:ssl, _}
             } = MQTT.default_config(server_opts: [transport: :ssl])
    end
  end

  describe "config/1" do
    test "returns the mqtt config" do
      assert %Config{
               client_id_prefix: "obmp",
               server: {:tcp, [host: "localhost", port: 1883]}
             } = MQTT.config()
    end

    test "sets transport" do
      assert %Config{
               server: {:ssl, _}
             } = MQTT.config(server_opts: [transport: :ssl])
    end
  end
end
