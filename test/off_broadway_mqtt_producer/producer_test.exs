defmodule OffBroadway.MQTTProducer.ProducerTest do
  use OffBroadway.MQTTProducerCase, async: true

  import ExUnit.CaptureLog

  alias OffBroadway.MQTTProducer.Producer

  @moduletag capture_log: true
  @moduletag start_supervisor: true
  @moduletag start_registry: true

  describe "init/1" do
    test "returns the expected state", context do
      %{client_id_prefix: prefix} = config = config_from_context(context)
      topic = to_string(context.test)

      assert {:producer,
              %{
                client_id: client_id,
                config: ^config,
                demand: 0,
                dequeue_timer: nil,
                queue: {:via, Registry, {registry, ^topic}} = queue_name
              }} = Producer.init([config, subscription: {topic, 0}])

      assert registry == config.queue_registry
      assert String.starts_with?(client_id, prefix)
    end

    test "passes the client id", context do
      config = config_from_context(context)
      topic = to_string(context.test)
      client_id = "test_id_#{System.unique_integer([:positive])}"

      assert {:producer, %{client_id: ^client_id}} =
               Producer.init([
                 config,
                 subscription: {topic, 0},
                 client_id: client_id
               ])
    end

    test "starts a registered and supervised queue", context do
      config = config_from_context(context)
      topic = to_string(context.test)

      Producer.init([config, subscription: {topic, 0}])
      assert [{pid, _}] = Registry.lookup(config.queue_registry, topic)

      assert [{_, ^pid, _, _}] =
               DynamicSupervisor.which_children(config.queue_supervisor)
    end

    @tag start_queue: "already_started_queue"
    test "logs a warning if a queue is already started", context do
      config = config_from_context(context)
      topic = context.queue_topic

      log =
        capture_log([level: :warn], fn ->
          assert {:producer, _} =
                   Producer.init([config, subscription: {topic, 0}])
        end)

      assert log =~ "already started"
    end

    test "starts a tortoise connection", context do
      config = config_from_context(context)

      topic = to_string(context.test)

      {_, %{client_id: client_id}} =
        Producer.init([config, subscription: {topic, 0}])

      assert_mqtt_client_running(client_id)
    end

    @tag start_mqtt_client: "already_started_client"
    test "returns error if client is already running", context do
      config = config_from_context(context)

      topic = to_string(context.test)

      {:stop, {:client, :already_started}} =
        Producer.init([
          config,
          subscription: {topic, 0},
          client_id: "already_started_client"
        ])
    end
  end

  describe "handle_demand/2" do
    setup context do
      config = config_from_context(context)
      topic = to_string(context.test)

      {:producer, state} = Producer.init([config, subscription: {topic, 0}])
      Map.put(context, :state, state)
    end

    test "updates the demand if it was unable to server", %{state: state} do
      assert {:noreply, [], %{demand: 1} = new_state} =
               Producer.handle_demand(1, state)

      assert {:noreply, [], %{demand: 2} = new_state} =
               Producer.handle_demand(1, new_state)

      assert {:noreply, [], %{demand: 12}} =
               Producer.handle_demand(10, new_state)
    end

    test "sets the dequeue timer", %{state: state} do
      assert {:noreply, [], new_state} = Producer.handle_demand(1, state)
      assert is_reference(new_state.dequeue_timer)
    end

    test "sends a dequeue message to itself", %{state: state} do
      assert {:noreply, [], _new_state} = Producer.handle_demand(1, state)
      assert_receive :dequeue_messages, 500
    end

    test "dequeues in the given interval", %{state: state} do
      state = update_config(state, dequeue_interval: 1000)
      assert {:noreply, [], _new_state} = Producer.handle_demand(1, state)
      refute_receive _, 900
      assert_receive :dequeue_messages, 200
    end

    test "does not update the dequeue_timer if already scheduled", %{
      state: state
    } do
      state = update_config(state, dequeue_interval: 1000)
      assert {:noreply, [], new_state} = Producer.handle_demand(1, state)
      refute_receive _, 500
      assert {:noreply, [], new_state} = Producer.handle_demand(1, state)
      refute_receive _, 400
      assert_receive :dequeue_messages, 200
    end

    test "returns message from the queue", %{state: state} do
      :ok = Queue.enqueue(state.queue, "test")

      assert {:noreply, ["test"], %{demand: 0}} =
               Producer.handle_demand(1, state)
    end

    test "dequeues multiple messages", %{state: state} do
      :ok = Queue.enqueue(state.queue, "test")
      :ok = Queue.enqueue(state.queue, "test")
      :ok = Queue.enqueue(state.queue, "test")

      assert {:noreply, [_, _, _], %{demand: 7} = state} =
               Producer.handle_demand(10, state)
    end

    test "properly updates the demand", %{state: state} do
      for n <- 1..13 do
        :ok = Queue.enqueue(state.queue, n)
      end

      assert {:noreply, [1, 2, 3, 4, 5], %{demand: 0} = state} =
               Producer.handle_demand(5, state)

      assert {:noreply, [6, 7, 8, 9, 10], %{demand: 0} = state} =
               Producer.handle_demand(5, state)

      assert {:noreply, [11, 12, 13], %{demand: 2} = state} =
               Producer.handle_demand(5, state)

      for n <- 1..10 do
        :ok = Queue.enqueue(state.queue, n)
      end

      assert {:noreply, [1, 2, 3, 4, 5, 6, 7], %{demand: 0} = state} =
               Producer.handle_demand(5, %{state | dequeue_timer: nil})
    end

    test "does not poll the queue if dequeue_timer is set", %{state: state} do
      assert {:noreply, [], %{demand: 1} = state} =
               Producer.handle_demand(1, state)

      for n <- 1..10 do
        :ok = Queue.enqueue(state.queue, n)
      end

      assert {:noreply, [], %{demand: 2} = state} =
               Producer.handle_demand(1, state)
    end
  end

  describe "terminate/2" do
    test "stops the mqtt client on terminate", context do
      Process.flag(:trap_exit, true)
      client_id = build_test_client_id()
      config = config_from_context(context)
      topic = to_string(context.test)

      {:ok, pid} =
        GenStage.start_link(Producer, [
          config,
          subscription: {topic, 0},
          client_id: client_id
        ])

      assert_mqtt_client_running(client_id)
      stop_and_receive_exit(pid, :normal)
      refute_mqtt_client_running(client_id)
    end

    test "does not stop the queue on terminate", context do
      Process.flag(:trap_exit, true)
      client_id = build_test_client_id()
      config = config_from_context(context)
      topic = to_string(context.test)

      {:ok, pid} =
        GenStage.start_link(Producer, [
          config,
          subscription: {topic, 0},
          client_id: client_id
        ])

      assert_mqtt_client_running(client_id)
      [{queue_pid, _}] = Registry.lookup(config.queue_registry, topic)

      stop_and_receive_exit(pid, :normal)
      assert Process.alive?(queue_pid)
    end
  end

  defp update_config(%{config: config} = state, opts) do
    overrides = Enum.into(opts, %{})

    updated_config = Map.merge(config, overrides)

    %{state | config: updated_config}
  end

  defp stop_and_receive_exit(pid, reason, timeout \\ 5000) do
    GenStage.stop(pid, reason)

    receive do
      {:EXIT, ^pid, _} -> true
    after
      timeout -> raise "never stopped"
    end
  end
end
