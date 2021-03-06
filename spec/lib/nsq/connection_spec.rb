# -*- encoding: utf-8 -*-

describe Nsq::Connection do

  describe '#new' do
    it 'should raise an exception if it cannot connect to nsqd' do
      cluster.halt!
      expect {
        Nsq::Connection.new host: host, port: tcp_port
      }.to raise_error Errno::ECONNREFUSED
    end
  end

  it 'should raise an exception if it connects to something that isn\'t nsqd' do
    expect {
      Nsq::Connection.new(host: host, port: http_port)
    }.to raise_error RuntimeError, /Bad frame type specified/
  end

    it 'should raise an exception if max_in_flight is above what the server supports' do
      expect {
        Nsq::Connection.new(host: host, port: tcp_port, max_in_flight: 1_000_000)
      }.to raise_error RuntimeError, /max_in_flight is set to 1000000, server only supports/
    end


  describe '#close' do
    it 'can be called multiple times, without issue' do
      expect {
        10.times { connection.close }
      }.not_to raise_error
    end
  end


  # This is really testing the ability for Connection to reconnect
  describe '#connected?' do
    before(:each) { set_speedy_connection_timeouts! }
    let(:conn){ Nsq::Connection.new host: host, port: tcp_port }
    it 'should return true when nsqd is up and false when nsqd is down' do
      wait_for(10, 'connected') { conn.connected? }
      expect(conn.connected?).to eq true
      nsqd.stop
      wait_for(10, 'disconnected') { !conn.connected? }
      expect(conn.connected?).to eq false
      nsqd.start
      wait_for(10, 'connected') { conn.connected? }
      expect(conn.connected?).to eq true
    end
  end

  describe 'private methods' do
    describe '#frame_class_for_type' do
      MAX_VALID_TYPE = described_class::FRAME_CLASSES.length - 1
      it "returns a frame class for types 0-#{MAX_VALID_TYPE}" do
        (0..MAX_VALID_TYPE).each do |type|
          expect(
            described_class::FRAME_CLASSES.include?(
              connection.send(:frame_class_for_type, type)
            )
          ).to be_truthy
        end
      end
      it "raises an error if invalid type > #{MAX_VALID_TYPE} specified" do
        expect {
          connection.send(:frame_class_for_type, 3)
        }.to raise_error(RuntimeError)
      end
    end


    describe '#handle_response' do
      it 'responds to heartbeat with NOP' do
        frame = Nsq::Response.new(described_class::RESPONSE_HEARTBEAT, connection)
        expect(connection).to receive(:nop)
        connection.send(:handle_response, frame)
      end
    end
  end
end
