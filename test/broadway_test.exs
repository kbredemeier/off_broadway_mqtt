defmodule OffBroadway.MQTTProducer.BroadwayTest do
  use OffBroadway.MQTTProducerCase, async: true

  alias OffBroadway.MQTTProducer.TestBroadway

  @moduletag capture_log: true

  describe "#{inspect(TestBroadway)}" do
    @tag start_registry: true, start_supervisor: true
    test "start_link/1 starts a process",
         context do
      opts = test_broadway_opts_from_context(context)
      assert {:ok, pid} = TestBroadway.start_link(opts)

      assert Process.alive?(pid)
    end

    @tag start_registry: true, start_supervisor: true
    test "start_link/1 connects to broker and subscribes to topic", context do
      opts = test_broadway_opts_from_context(context)

      assert {:ok, _pid} = TestBroadway.start_link(opts)
      assert_receive {:subscription, _client_id, topic, :up}
      assert topic && topic == opts[:topic]
    end

    @tag start_registry: true, start_supervisor: true
    test "invokes the process_fun", context do
      opts = test_broadway_opts_from_context(context)
      assert {:ok, pid} = TestBroadway.start_link(opts)

      data = wrap_data("test", opts[:topic])
      Broadway.test_messages(pid, [data])

      assert_receive {:process_fun, ^data}
    end

    @tag start_registry: true, start_supervisor: true
    test "adds exception as error to message if processor fails", context do
      process_fun = fn _ ->
        raise "Fooo"
      end

      opts = test_broadway_opts_from_context(context, process_fun: process_fun)
      assert {:ok, pid} = TestBroadway.start_link(opts)

      data = wrap_data("test", opts[:topic])
      ref = Broadway.test_messages(pid, [data])

      assert_receive {:ack, ^ref, [],
                      [%{status: {:failed, %RuntimeError{}}, data: ^data}]},
                     5000
    end

    @tag start_registry: true, start_supervisor: true
    test "invokes the batch_fun", context do
      opts = test_broadway_opts_from_context(context)
      assert {:ok, pid} = TestBroadway.start_link(opts)

      data = wrap_data("test", opts[:topic])
      Broadway.test_messages(pid, [data])

      assert_receive {:batch_fun, [%{data: ^data}]}, 5000
    end

    @tag start_registry: true, start_supervisor: true
    test "adds exception as error to message if batcher fails", context do
      batch_fun = fn _ ->
        raise "Fooo"
      end

      opts = test_broadway_opts_from_context(context, batch_fun: batch_fun)
      assert {:ok, pid} = TestBroadway.start_link(opts)

      data = wrap_data("test", opts[:topic])
      ref = Broadway.test_messages(pid, [data])

      assert_receive {:ack, ^ref, [],
                      [%{status: {:failed, %RuntimeError{}}, data: ^data}]},
                     5000
    end

    @tag start_registry: true, start_supervisor: true
    test "processes a message successfully", context do
      opts = test_broadway_opts_from_context(context)
      assert {:ok, pid} = TestBroadway.start_link(opts)

      data = wrap_data("test", opts[:topic])
      ref = Broadway.test_messages(pid, [data])
      assert_receive {:ack, ^ref, [%{data: ^data}], []}, 5000
    end

    @tag start_registry: true, start_supervisor: true, start_mqtt_client: true
    test "processes messages from mqtt", context do
      opts = test_broadway_opts_from_context(context)
      assert {:ok, pid} = TestBroadway.start_link(opts)
      assert_receive {:subscription, _client_id, _topic, :up}

      expected_data = wrap_data("Hello, World!", opts[:topic])
      Tortoise.publish(context.test_client_id, opts[:topic], "Hello, World!")
      assert_receive {:process_fun, ^expected_data}, 2000
      assert_receive {:batch_fun, [%{data: ^expected_data}]}, 2000
    end

    @tag start_registry: true, start_supervisor: true, start_mqtt_client: true
    test "batches as configured", context do
      opts = test_broadway_opts_from_context(context)
      assert {:ok, pid} = TestBroadway.start_link(opts)
      assert_receive {:subscription, _client_id, _topic, :up}

      expected_data = wrap_data("Hello, World!", opts[:topic])

      for _ <- 1..10 do
        Tortoise.publish(context.test_client_id, opts[:topic], "Hello, World!")
        Process.sleep(50)
      end

      assert_receive {:batch_fun, batched_messages}, 5000
      assert Enum.count(batched_messages) > 5

      for msg <- batched_messages do
        assert msg.data == expected_data
        assert msg.status == :ok
      end
    end
  end
end
