# -*- encoding: utf-8 -*-
require 'json'
require 'timeout'

describe Nsq::Consumer do

  describe 'when connecting to nsqd directly' do
    let(:consumer_options) { { nsqlookupd: nil, nsqd: nsqd.address, max_in_flight: 10 } }
    describe '#new' do
      it "should throw an exception when trying to connect to a server that's down" do
        nsqd.stop
        expect { consumer }.to raise_error Errno::ECONNREFUSED, /Connection refused - connect/
      end
    end
    # This is testing the behavior of the consumer, rather than the size method itself
    describe '#size' do
      it 'doesn\'t exceed max_in_flight for the consumer' do
        # publish a bunch of messages
        (consumer.max_in_flight * 2).times { connection.pub consumer.topic, 'some-message' }

        wait_for(10, 'all connections up') { consumer.size >= consumer.max_in_flight }
        expect(consumer.size).to eq(consumer.max_in_flight)
      end
    end
    describe '#pop' do
      it 'can pop off a message' do
        connection.pub consumer.topic, 'some-message'
        assert_no_timeout(1) do
          msg = consumer.pop
          expect(msg.body).to eq 'some-message'
          msg.finish
        end
      end
      it 'can pop off many messages' do
        10.times { connection.pub consumer.topic, 'some-message' }
        assert_no_timeout(1) do
          10.times { consumer.pop.finish }
        end
      end
      it 'can receive messages with unicode characters' do
        connection.pub consumer.topic, '☺'
        expect(consumer.pop.body).to eq('☺')
      end
    end
    describe '#req' do
      it 'can successfully requeue a message' do
        # queue a message
        connection.pub topic, 'twice'
        msg = consumer.pop
        expect(msg.body).to eq 'twice'
        # requeue it
        msg.requeue
        req_msg = consumer.pop
        expect(req_msg.body).to eq 'twice'
        expect(req_msg.attempts).to eq 2
      end
    end
  end
  describe 'when using lookupd' do
    let(:exp_qty) { 20 }
    let!(:exp_messages) do
      (1..exp_qty).to_a.map(&:to_s).tap do |messages|
        messages.each_with_index do |message, idx|
            cluster.nsqd_for_idx(idx).conn { |conn| conn.pub(topic, message) }
        end
      end
    end
    let(:consumer_options) { { max_in_flight: 10 } }
    describe '#pop' do
      it 'receives messages from both queues' do
        messages = []
        assert_no_timeout(5) do
          exp_qty.times do
            msg = consumer.pop
            messages << msg.body
            msg.finish
          end
        end
        expect(messages.sort).to eq(exp_messages.sort)
      end
    end
    # This is testing the behavior of the consumer, rather than the size method itself
    describe '#size' do
      it 'doesn\'t exceed max_in_flight for the consumer' do
        wait_for(10, 'all connections up') { consumer.size >= consumer.max_in_flight }
        expect(consumer.size).to eq(consumer.max_in_flight)
      end
    end
  end
  describe 'with a low message timeout' do
    let(:msg_timeout) { 1 }
    let!(:consumer_options) { { nsqlookupd: nil, nsqd: nsqd_url, msg_timeout: msg_timeout * 1000 } }
    # This testing that our msg_timeout is being honored
    it 'should give us the same message over and over' do
      puts 'running ...'
      connection.pub topic, 'slow'
      puts 'published ...'

      msg1 = consumer.pop false
      puts 'polled ...'
      expect(msg1.body).to eq('slow')
      expect(msg1.attempts).to eq(1)

      # wait for it to be reclaimed by nsqd and then finish it so we can get another. this fin won't actually succeed,
      # because the message is no longer in flight
      sleep(msg_timeout + 0.5)
      msg1.finish
      puts 'finished ...'

      assert_no_timeout do
        msg2 = consumer.pop false
        puts 'polled again ...'
        expect(msg2.body).to eq('slow')
        expect(msg2.attempts).to eq(2)
      end
    end

    # This is like the test above, except we touch the message to reset its
    # timeout
    it 'should be able to touch a message to reset its timeout' do
      connection.pub(topic, 'slow')

      msg1 = consumer.pop
      expect(msg1.body).to eq('slow')

      # touch the message in the middle of a sleep session whose total just
      # exceeds the msg_timeout
      sleep(msg_timeout / 2.0 + 0.1)
      msg1.touch
      sleep(msg_timeout / 2.0 + 0.1)
      msg1.finish

      # if our touch didn't work, we should receive a message
      assert_timeout { consumer.pop }
    end


    describe '#drop_and_add_connections' do
      context 'lookupd returns nsqd instances that are down' do
        it 'should log an error and not explode' do
          # expect an error log call
          expect(consumer).to receive(:error)

          expect {
            consumer.send(:drop_and_add_connections, ['127.0.0.1:4321'])
          }.to_not raise_error
        end
      end
    end
  end
  describe 'with a high max_in_flight and tons of messages' do
    let(:qty) { 10_000 }
    let(:slice_size) { 100 }
    let(:expected_messages) { (1..qty).to_a.map(&:to_s) }
    let(:write_messages) {
      expected_messages.each_slice(slice_size) do |slice|
        cluster.nsqds.sample.conn { |conn| conn.mpub topic, slice }
      end
    }
    let(:consumer) { new_consumer max_in_flight: slice_size * 10 }
    let(:received_messages) { [] }
    let(:read_messages) {
      expected_messages.length.times do
        consumer.pop.tap { |msg| received_messages << msg.body; msg.finish }
      end
    }
    it 'should receive all messages in a reasonable amount of time' do
      write_messages
      assert_no_timeout(5) { read_messages }
      consumer.terminate
      expect(received_messages.sort).to eq(expected_messages.sort)
    end
  end
  describe 'when using bad topic names' do
    it 'should throw an exception for long names' do
      expect { new_consumer(topic: 'consumer-topic-longer-than-64-very-long-and_unnecessary-characters') }.to raise_error(ArgumentError, 'invalid topic name')
    end
    it 'should throw an exception for invalid (characters) names' do
      expect { new_consumer(topic: 'topic-is-^*&-whack') }.to raise_error(ArgumentError, 'invalid topic name')
    end
  end
  describe 'when using bad channel names' do
    it 'should throw an exception for long names' do
      expect { new_consumer(channel: 'consumer-cheannel-longer-than-64-very-long-and_unnecessary-characters') }.to raise_error(ArgumentError, 'invalid channel name')
    end
    it 'should throw an exception for invalid (characters) names' do
      expect { new_consumer(channel: 'channel-is-^*&-whack') }.to raise_error(ArgumentError, 'invalid channel name')
    end
  end
  describe 'when waiting for empty queue' do
    it 'should wait indefinitely without a timeout (default)' do
      result = nil
      thr    = Thread.new { result = consumer.pop }
      sleep 1.5
      connection.pub consumer.topic, 'some-message'
      sleep 0.25
      thr.join
      expect(result).to be_a Nsq::Message
      expect(result.body).to eq 'some-message'
    end
    it 'should throw an exception after timeout expires' do
      expect { consumer.pop(0.5) }.to raise_error ThreadError, /queue empty/
    end
  end
end
