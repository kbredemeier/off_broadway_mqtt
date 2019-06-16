defmodule OffBroadway.MQTTProducer.ClientTest do
  use OffBroadway.MQTTProducerCase, async: true

  alias OffBroadway.MQTTProducer.Client
  alias OffBroadway.MQTTProducer.Config
  alias OffBroadway.MQTTProducer.TestHandler

  @moduletag capture_log: true
  @moduletag start_supervisor: true
  @moduletag start_registry: true
  @moduletag start_queue: true

  test "starts a process", %{queue: queue, queue_topic: topic} do
    assert {:ok, pid} = Client.start(:default, {topic, 0}, queue)

    assert Process.alive?(pid)
    refute_receive _, 1000
  end

  test "sends a message if subscribed", %{queue: queue, queue_topic: topic} do
    client_id = "test_client_#{System.unique_integer([:positive])}"

    assert {:ok, _pid} =
             Client.start(:default, {topic, 0}, queue,
               sub_ack: self(),
               client_id: client_id
             )

    assert_receive_sub_ack(client_id, topic)
  end

  test "generates a client_id", %{queue: queue, queue_topic: topic} do
    assert {:ok, _pid} =
             Client.start(:default, {topic, 0}, queue, sub_ack: self())

    assert_receive {:subscription, client_id, _, :up}
    assert String.starts_with?(client_id, "off_broadway")
  end

  test "starts a tortoise connection", %{queue: queue, queue_topic: topic} do
    assert {:ok, _pid} =
             Client.start(:default, {topic, 0}, queue, sub_ack: self())

    assert_receive {:subscription, client_id, _, :up}
    assert_mqtt_client_running(client_id)
  end

  test "uses the handler module from the config", %{
    queue: queue,
    queue_topic: topic
  } do
    config = :default |> Config.new() |> Map.put(:handler, TestHandler)

    assert {:ok, _pid} =
             Client.start(config, {topic, 0}, queue, handler_opts: [pid: self()])

    assert_receive {:test_mqtt_client, :up}, 2000
  end

  describe "telemetry events" do
    test "sends telemetry event if connection comes up",
         %{
           queue_topic: topic,
           queue: queue
         } do
      test_pid = self()

      handle_event = fn event, topic, data, extra ->
        send(test_pid, {:telemetry, event, topic, data, extra})
      end

      event = [:off_broadway_mqtt_producer, :client, :connection, :up]
      :telemetry.attach("test", event, handle_event, nil)

      Client.start(:default, {topic, 0}, queue)
      assert_receive {:telemetry, ^event, %{count: 1}, meta, nil}
      assert meta.topic_filter == topic
      assert meta.qos
      assert meta.client_id
      assert meta.host
      assert meta.port
    end
  end
end
