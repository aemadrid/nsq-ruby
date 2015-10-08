describe Nsq::Producer do
  context 'connecting directly to a single nsqd' do
    let(:cluster_options) { { nsqds: 1 } }
    let!(:producer) { @producer = new_producer producer_options }
    describe '#new' do
      it 'should throw an exception when trying to connect to a server that\'s down' do
        nsqd.stop
        expect { new_producer }.to raise_error Errno::ECONNREFUSED, /Connection refused/
      end
    end
    describe '#connected?' do
      it 'should return true when it is connected' do
        expect(producer.connected?).to eq(true)
      end
      it 'should return false when nsqd is down' do
        nsqd.stop
        wait_for { !producer.connected? }
        expect(producer.connected?).to eq false
      end
    end
    describe '#write' do
      it 'can queue a message' do
        producer.write 'some-message'
        wait_for { message_count == 1 }
        expect(message_count).to eq 1
      end
      it 'can queue multiple messages at once' do
        producer.write(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
        wait_for { message_count == 10 }
        expect(message_count).to eq 10
      end
      it 'shouldn\'t raise an error when nsqd is down' do
        nsqd.stop
        expect { 10.times { producer.write('fail') } }.to_not raise_error
      end
      it 'will attempt to resend messages when it reconnects to nsqd' do
        nsqd.stop

        # Write 10 messages while nsqd is down
        10.times { |i| producer.write(i) }

        nsqd.start

        messages_received = []

        begin
          wait_for(10, 'nsq is listening') { http_producer.ping.success? }
          consumer = new_nsqd_consumer
          assert_no_timeout(5) do
            # TODO: make the socket fail faster
            # We only get 8 or 9 of the 10 we send. The first few can be lost
            # because we can't detect that they didn't make it.
            8.times do
              msg = consumer.pop
              messages_received << msg.body
              msg.finish
            end
          end
        ensure
          consumer.terminate if consumer
        end

        expect(messages_received.uniq.length).to eq(8)
      end
      # Test PUB
      it 'can send a single message with unicode characters' do
        producer.write('☺')
        consumer = new_nsqd_consumer
        assert_no_timeout { expect(consumer.pop.body).to eq('☺') }
      end
      # Test MPUB as well
      it 'can send multiple message with unicode characters' do
        producer.write('☺', '☺', '☺')
        consumer = new_nsqd_consumer
        assert_no_timeout do
          3.times do
            msg = consumer.pop
            expect(msg.body).to eq('☺')
            msg.finish
          end
        end
      end
    end
  end
  context 'connecting via nsqlookupd' do
    let(:nsqd_count) { 2 }
    let(:cluster_options) { { nsqds: nsqd_count, lookupds: 1 } }
    let(:producer) { new_lookupd_producer }
    let(:timeout) { 25 }
    before(:each) do
      wait_for(timeout, 'get the same producer/nsqds connections') do
        producer.connections.length == cluster.nsqds.length
      end
    end
    describe '#connections' do
      it 'should be connected to all nsqds' do
        expect(producer.connections.length).to eq(cluster.nsqds.length)
      end
      it 'should drop a connection when an nsqd goes offline' do
        cluster.nsqds.first.stop
        wait_for(30, 'get the actual producer/nsqds connections') do
          producer.connections.length == cluster.nsqds.length - 1
        end
        expect(producer.connections.length).to eq(nsqd_count - 1)
      end
    end
    describe '#connected?' do
      it 'should return true if it\'s connected to at least one nsqd' do
        expect(producer.connected?).to eq true
      end
      it 'should return false when it\'s not connected to any nsqds' do
        cluster.nsqds.each { |nsqd| nsqd.stop }
        wait_for { !producer.connected? }
        expect(producer.connected?).to eq(false)
      end
    end
    describe '#write' do
      it 'writes to a random connection' do
        expect_any_instance_of(Nsq::Connection).to receive :pub
        producer.write 'howdy!'
      end
      it 'raises an error if there are no connections to write to' do
        cluster.nsqds.each { |nsqd| nsqd.stop }
        wait_for { producer.connections.length == 0 }
        expect { producer.write('die') }.to raise_error RuntimeError, 'No connections available'
      end
    end
  end
  describe 'when using bad topic names' do
    it 'should throw an exception for long names' do
      expect { Nsq::Producer.new(nsqd: 'localhost:4150', topic: 'consumer-topic-longer-than-64-very-long-and-unnecessary-characters') }.to raise_error(ArgumentError, 'invalid topic name')
    end
    it 'should throw an exception for invalid (characters) names' do
      expect { Nsq::Producer.new(nsqd: 'localhost:4150', topic: 'topic-is-^*&-whack') }.to raise_error(ArgumentError, 'invalid topic name')
    end
  end
end
