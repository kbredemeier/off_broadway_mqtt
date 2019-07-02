defmodule OffBroadway.MQTT.TelemetryTest do
  use OffBroadway.MQTTCase, async: false

  @moduletag start_supervisor: true
  @moduletag start_registry: true
  @moduletag build_config: true

  describe "client events" do
    alias OffBroadway.MQTT.Producer

    @tag subscribe_telemetry_event: [:test, :client, :connection, :up]
    test "sends telemetry event if connection comes up",
         %{event: event} = context do
      topic = to_string(context.test)
      Producer.init([context.config, subscription: {topic, 0}])
      assert_receive {:telemetry, ^event, %{count: 1}, meta, nil}, 5000
      assert meta.topic_filter == topic
      assert meta.qos
      assert meta.client_id
      assert meta.host
      assert meta.port
    end

    @tag subscribe_telemetry_event: [:test, :client, :subscription, :up]
    test "sends telemetry event if subscription comes up",
         %{event: event} = context do
      topic = to_string(context.test)
      Producer.init([context.config, subscription: {topic, 0}])
      assert_receive {:telemetry, ^event, %{count: 1}, meta, nil}, 5000
      assert meta.topic_filter == topic
      assert meta.qos
      assert meta.client_id
      assert meta.host
      assert meta.port
    end

    @tag subscribe_telemetry_event: [:test, :client, :messages],
         start_mqtt_client: true
    test "sends telemetry event if message received",
         %{event: event} = context do
      topic = to_string(context.test)
      Producer.init([context.config, subscription: {topic, 0}, sub_ack: self()])
      assert_receive_sub_ack(_, topic)
      Tortoise.publish(context.test_client_id, topic, "Hello, World!")
      assert_receive {:telemetry, ^event, %{count: 1}, meta, nil}, 5000
      assert meta.topic_filter == topic
      assert meta.qos
      assert meta.client_id
      assert meta.host
      assert meta.port
    end
  end

  describe "queue events" do
    @tag subscribe_telemetry_event: [:test, :queue, :in],
         start_queue: true
    test "sends telemetry event if value enqueued",
         %{queue_topic: topic, queue: queue, event: event} do
      :ok = Queue.enqueue(queue, "test")
      assert_receive {:telemetry, ^event, %{count: 1, size: 1}, meta, nil}, 1000
      assert meta.topic_filter == topic
      :ok = Queue.enqueue(queue, "test")
      assert_receive {:telemetry, ^event, %{count: 1, size: 2}, _, nil}, 1000
      :ok = Queue.enqueue(queue, "test")
      assert_receive {:telemetry, ^event, %{count: 1, size: 3}, _, nil}, 1000
    end

    @tag subscribe_telemetry_event: [:test, :queue, :out],
         start_queue: true
    test "sends telemetry event if value dequeued",
         %{queue_topic: topic, queue: queue, event: event} do
      for n <- 1..10 do
        :ok = Queue.enqueue(queue, n)
      end

      Queue.dequeue(queue, 1)
      assert_receive {:telemetry, ^event, %{count: 1, size: 9}, meta, nil}, 1000
      assert meta.topic_filter == topic

      Queue.dequeue(queue, 5)
      assert_receive {:telemetry, ^event, %{count: 5, size: 4}, meta, nil}, 1000
    end
  end

  describe "acknowledger events" do
    alias OffBroadway.MQTT.Acknowledger, as: Ack
    import OffBroadway.MQTT

    @tag subscribe_telemetry_event: [:test, :acknowledger, :failed],
         start_queue: true
    test "sends telemetry event if message failed with exception", %{
      queue: queue,
      queue_topic: topic,
      event: event
    } do
      failed_msg =
        "test"
        |> wrap_data(topic)
        |> wrap_msg(queue)
        |> fail_msg(%ArgumentError{message: "Argh!"})

      assert :ok = Ack.ack(topic, [], [failed_msg])
      assert_receive {:telemetry, ^event, %{count: 1}, _meta, nil}, 1000
    end

    @tag subscribe_telemetry_event: [:test, :acknowledger, :failed],
         start_queue: true
    test "sends telemetry event if message failed with error tuple", %{
      queue: queue,
      queue_topic: topic,
      event: event
    } do
      failed_msg =
        "test"
        |> wrap_data(topic)
        |> wrap_msg(queue)
        |> fail_msg({:error, :foo})

      assert :ok = Ack.ack(topic, [], [failed_msg])
      assert_receive {:telemetry, ^event, %{count: 1}, _meta, nil}, 1000
    end

    @tag subscribe_telemetry_event: [:test, :acknowledger, :ignored],
         start_queue: true
    test "sends telemetry event if message is ignored", %{
      queue: queue,
      queue_topic: topic,
      event: event
    } do
      failed_msg =
        "test"
        |> wrap_data(topic)
        |> wrap_msg(queue)
        |> fail_msg(%{ack: :ignore})

      assert :ok = Ack.ack(topic, [], [failed_msg])
      assert_receive {:telemetry, ^event, %{count: 1}, _meta, nil}, 1000
    end

    @tag subscribe_telemetry_event: [:test, :acknowledger, :requeued],
         start_queue: true
    test "sends telemetry event if message is requeued", %{
      queue: queue,
      queue_topic: topic,
      event: event
    } do
      failed_msg =
        "test"
        |> wrap_data(topic)
        |> wrap_msg(queue)
        |> fail_msg(%{ack: :retry})

      assert :ok = Ack.ack(topic, [], [failed_msg])
      assert_receive {:telemetry, ^event, %{count: 1}, _meta, nil}, 1000
    end

    @tag subscribe_telemetry_event: [:test, :acknowledger, :success],
         start_queue: true
    test "sends telemetry event if message is successful", %{
      queue: queue,
      queue_topic: topic,
      event: event
    } do
      msg =
        "test"
        |> wrap_data(topic)
        |> wrap_msg(queue)

      assert :ok = Ack.ack(topic, [msg], [])
      assert_receive {:telemetry, ^event, %{count: 1}, _meta, nil}, 1000
    end
  end
end
