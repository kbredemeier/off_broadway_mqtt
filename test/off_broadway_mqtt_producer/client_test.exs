defmodule OffBroadway.MQTTProducer.ClientTest do
  use OffBroadway.MQTTProducerCase, async: true

  alias OffBroadway.MQTTProducer.Client
  alias OffBroadway.MQTTProducer.Config
  alias OffBroadway.MQTTProducer.TestHandler

  @moduletag capture_log: true

  @tag start_registry: true
  @tag start_queue: true
  test "starts a process", %{queue: queue, queue_topic: topic} do
    assert {:ok, pid} = Client.start(:default, {topic, 0}, queue)

    assert Process.alive?(pid)
    refute_receive _, 1000
  end

  @tag start_registry: true
  @tag start_queue: true
  test "sends a message if subscribed", %{queue: queue, queue_topic: topic} do
    assert {:ok, _pid} =
             Client.start(:default, {topic, 0}, queue, sub_ack: self())

    assert_receive {:subscription, _, ^topic, :up}, 2000
  end

  @tag start_registry: true
  @tag start_queue: true
  test "uses the handler module from the config", %{
    queue: queue,
    queue_topic: topic
  } do
    config = :default |> Config.new() |> Map.put(:handler, TestHandler)

    assert {:ok, _pid} =
             Client.start(config, {topic, 0}, queue, handler_opts: [pid: self()])

    assert_receive {:test_mqtt_client, :up}, 2000
  end
end
