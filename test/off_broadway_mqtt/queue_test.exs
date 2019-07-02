defmodule OffBroadway.MQTT.QueueTest do
  use OffBroadway.MQTTCase, async: true

  @moduletag build_config: true

  @tag start_registry: true
  test "start_link/1 starts a queue under the given name", %{
    config: config,
    registry: registry,
    test: test
  } do
    assert {:ok, pid} =
             Queue.start_link([
               config,
               {:via, Registry, {registry, test}}
             ])

    assert Process.alive?(pid)
    assert [{^pid, _}] = Registry.lookup(registry, test)
  end

  @tag :start_queue
  test "enqueue/2 returns :ok", %{pid: pid} do
    assert :ok = Queue.enqueue(pid, "test")
  end

  @tag :start_queue
  test "dequeue/2 returns the demanded amount of enqueue items", %{pid: pid} do
    for n <- 1..100 do
      assert :ok = Queue.enqueue(pid, n)
    end

    assert Enum.into(1..100, []) == Queue.dequeue(pid, 100)
  end

  @tag :start_queue
  test "dequeue/2 returns not more than the requested amount of items", %{
    pid: pid
  } do
    for n <- 1..100 do
      assert :ok = Queue.enqueue(pid, n)
    end

    assert Enum.into(1..50, []) == Queue.dequeue(pid, 50)
    assert Enum.into(51..100, []) == Queue.dequeue(pid, 50)
  end

  @tag :start_queue
  test "dequeue/2 returns the remaining items if demanded more than available",
       %{
         pid: pid
       } do
    for n <- 1..10 do
      assert :ok = Queue.enqueue(pid, n)
    end

    assert Enum.into(1..10, []) == Queue.dequeue(pid, 50)
    assert [] == Queue.dequeue(pid, 50)
  end

  @tag :start_queue
  test "dequeue/2 returns empty list if queue empty", %{pid: pid} do
    assert [] == Queue.dequeue(pid, 50)
  end

  for thing <- ["foo", :bar, 1, [1, 2, 3], nil, [], <<"baz">>] do
    @tag :start_queue
    test "queues and dequeues #{inspect(thing)}", %{pid: pid} do
      assert :ok = Queue.enqueue(pid, unquote(thing))
      assert [unquote(thing)] == Queue.dequeue(pid, 1)
    end
  end
end
