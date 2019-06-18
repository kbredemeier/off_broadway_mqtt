defmodule OffBroadway.MQTTProducer.ClientTest do
  use OffBroadway.MQTTProducerCase, async: true

  import ExUnit.CaptureLog

  alias OffBroadway.MQTTProducer.Client
  alias OffBroadway.MQTTProducer.Config
  alias OffBroadway.MQTTProducer.TestHandler

  @moduletag start_supervisor: true
  @moduletag start_registry: true
  @moduletag start_queue: true
  @moduletag build_config: true

  test "starts a process", %{queue: queue, queue_topic: topic, config: config} do
    assert {:ok, pid} = Client.start(config, {topic, 0}, queue)

    assert Process.alive?(pid)
    refute_receive _, 1000
  end

  test "logs a warning if client_id is larger than 23 bytes", %{
    queue: queue,
    queue_topic: topic,
    config: config
  } do
    log =
      capture_log([level: :warn], fn ->
        client_id = <<1::size(192)>>

        assert {:ok, pid} =
                 Client.start(config, {topic, 0}, queue, client_id: client_id)

        Process.sleep(1000)
      end)

    assert log =~ "larger than 23 bytes"
  end

  test "does not log a warning if client_id is smaller that 23 bytes", %{
    queue: queue,
    queue_topic: topic,
    config: config
  } do
    log =
      capture_log([level: :warn], fn ->
        client_id = <<1::size(184)>>

        assert {:ok, pid} =
                 Client.start(config, {topic, 0}, queue, client_id: client_id)
      end)

    refute log =~ "larger than 23 bytes"
  end

  test "sends a message if subscribed", %{
    queue: queue,
    queue_topic: topic,
    config: config
  } do
    client_id = "test_client_#{System.unique_integer([:positive])}"

    assert {:ok, _pid} =
             Client.start(config, {topic, 0}, queue,
               sub_ack: self(),
               client_id: client_id
             )

    assert_receive_sub_ack(client_id, topic)
  end

  test "generates a client_id", %{
    queue: queue,
    queue_topic: topic,
    config: config
  } do
    assert {:ok, _pid} =
             Client.start(config, {topic, 0}, queue, sub_ack: self())

    assert_receive {:subscription, client_id, _, :up}, 1000
    assert String.starts_with?(client_id, "test")
  end

  test "starts a tortoise connection", %{
    queue: queue,
    queue_topic: topic,
    config: config
  } do
    assert {:ok, _pid} =
             Client.start(config, {topic, 0}, queue, sub_ack: self())

    assert_receive {:subscription, client_id, _, :up}, 1000
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
end
