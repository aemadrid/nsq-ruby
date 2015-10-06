# -*- encoding: utf-8 -*-

describe Nsq::Consumer do
  let(:nsqd_count) { 3 }
  let(:cluster_options) { { lookupds: 2, nsqds: nsqd_count } }
  let!(:messages) { cluster.nsqds.each { |nsqd| nsqd.conn { |x| x.pub topic, 'hi' } } }
  let(:consumer_options) { { max_in_flight: 20, discovery_interval: 0.1 } }

  before(:each) do
    set_speedy_connection_timeouts!
    wait_for { consumer.connections.length == nsqd_count }
  end

  # This is really testing that the discovery loop works as expected.
  #
  # The consumer won't evict connections if they go down, the connection itself
  # will try to reconnect.
  #
  # But, when nsqd goes down, nsqlookupd will see that its gone and unregister
  # it. So when the next time the discovery loop runs, that nsqd will no longer
  # be listed.
  it 'should drop a connection when an nsqd goes down and add one when it comes back' do
    cluster.nsqds.last.stop
    wait_for { consumer.connections.length == nsqd_count - 1 }
    cluster.nsqds.last.start
    wait_for { consumer.connections.length == nsqd_count }
  end
  it 'should continue processing messages from live queues when one queue is down' do
    # shut down the last nsqd
    puts '> stopping nsqd #3 ...'
    cluster.nsqds.last.stop

    # make sure there are more messages on each queue than max in flight
    puts '> pushing 50 msgs to nsqd #1 ...'
    cluster.nsqds[0].conn { |x| 50.times { x.pub topic, 'hay' } }
    puts '> pushing 50 msgs to nsqd #2 ...'
    cluster.nsqds[1].conn { |x| 50.times { x.pub topic, 'hay' } }
    puts '> getting 100 msgs from the same connection in less than 5s ...'
    assert_no_timeout(5) do
      100.times do |n|
        puts "#{n + 1} ..."
        consumer.pop.finish
      end
    end
  end
  it 'should process messages from a new queue when it comes online', skip: true do
    nsqd = cluster.nsqds.last
    nsqd.stop

    thread = Thread.new do
      nsqd.start
      nsqd.conn { |x| x.pub topic, 'needle' }
    end

    assert_no_timeout(5) do
      string = nil
      until string == 'needle'
        msg    = consumer.pop
        string = msg.body
        msg.finish
      end
      true
    end

    thread.join
  end
  it 'should be able to rely on the second nsqlookupd if the first dies' do
    bad_lookupd = cluster.lookupds.first
    bad_lookupd.stop

    nsqd.conn { |x| x.pub 'new-topic', 'new message on new topic' }
    consumer = new_consumer topic: 'new-topic'

    assert_no_timeout do
      msg = consumer.pop
      expect(msg.body).to eq('new message on new topic')
      msg.finish
    end
  end
  it 'should be able to handle all queues going offline and coming back' do
    consumer
    expected_messages = cluster.nsqds.map { |nsqd| nsqd.tcp_port.to_s }

    puts "cluster : 0 #{cluster}"
    cluster.nsqds.each { |q| q.stop }
    puts "cluster : 1 #{cluster}"
    cluster.nsqds.each { |q| q.start }
    puts "cluster : 2 #{cluster}"

    cluster.nsqds.each_with_index do |nsqd, idx|
      nsqd.conn { |x| x.pub topic, expected_messages[idx] }
    end

    assert_no_timeout(10) do
      received_messages = []

      while (expected_messages & received_messages).length < expected_messages.length do
        msg = consumer.pop
        received_messages << msg.body
        msg.finish
      end
    end
  end
end

