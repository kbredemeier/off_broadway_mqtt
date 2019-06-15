defmodule OffBroadway.MQTTProducerTest do
  use OffBroadway.MQTTProducerCase, async: true

  alias OffBroadway.MQTTProducer.Config

  @moduletag capture_log: true

  describe "unique_client_id_/1" do
    test "returns a string" do
      assert id = MQTTProducer.unique_client_id()
      assert is_binary(id)
      assert Regex.match?(~r/^off_broadway_mqtt_producer_.+/, id)
    end

    test "returns unique client ids" do
      ids =
        for _ <- 1..100 do
          MQTTProducer.unique_client_id()
        end

      assert ids == Enum.uniq(ids)
    end

    test "takes the client_id_prefix from config" do
      assert id =
               MQTTProducer.unique_client_id(%{
                 client_id_prefix: "asd"
               })

      assert Regex.match?(~r/^asd_.+/, id)
    end
  end

  test "topic_from_queue_name/1" do
    assert "test" =
             MQTTProducer.topic_from_queue_name(
               {:via, Registry, {:foo, "test"}}
             )
  end

  describe "queue_name" do
    test "returns the process name for a topic queue server" do
      assert {:via, Registry, {:foo, "bar"}} ==
               MQTTProducer.queue_name(
                 %{queue_registry: :foo},
                 "bar"
               )
    end

    test "returns the process name with the default registry" do
      assert {:via, Registry, {MQTTProducer.QueueRegistry, "bar"}} ==
               MQTTProducer.queue_name("bar")
    end
  end

  describe "config/1" do
    test "returns the mqtt config" do
      assert %Config{
               client_id_prefix: "off_broadway_mqtt_producer",
               server: {:tcp, [host: 'vernemq', port: 1883]}
             } = MQTTProducer.config()
    end

    test "sets transport" do
      assert %Config{
               server: {:ssl, _}
             } = MQTTProducer.config(server_opts: [transport: :ssl])
    end
  end
end
