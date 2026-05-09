# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

require_relative "test_helper"

class TestSend < Minitest::Test
  include PgqueTest::Helpers

  def test_send_returns_int_event_id
    with_queue do |queue, _consumer, conn|
      client = Pgque::Client.new(conn)
      eid = client.send(queue, { "order_id" => 42 })
      assert_kind_of Integer, eid
      assert_operator eid, :>, 0
    end
  end
end
