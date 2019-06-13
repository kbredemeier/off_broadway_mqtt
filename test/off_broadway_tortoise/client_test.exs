defmodule OffBroadwayTortoise.ClientTest do
  use OffBroadwayTortoise.Case, async: true

  alias OffBroadwayTortoise.Client

  @moduletag capture_log: true

  @tag start_registry: true
  @tag start_queue: true
  test "returns :ok", %{queue: queue, queue_topic: topic} do
    assert :ok = Client.start(queue, {topic, 0})
    refute_receive _
  end

  @tag start_registry: true
  @tag start_queue: true
  test "sends a message if subscribed", %{queue: queue, queue_topic: topic} do
    assert :ok = Client.start(queue, {topic, 0}, nil, sub_ack: self())
    assert_receive {:subscription, _, _, :up}
  end

  @tag start_registry: true
  @tag start_queue: true
  test "starts a client for each subscription", %{
    queue: queue
  } do
    assert :ok =
             Client.start(queue, [{"test/foo", 0}, {"test/bar", 0}], nil,
               sub_ack: self()
             )

    assert_receive {:subscription, client_1, "test/foo", :up}
    assert_receive {:subscription, client_2, "test/bar", :up}
    refute client_1 == client_2
  end
end
