describe Nsq::Message do

  describe '#timestamp' do
    it 'should be a Time object' do
      connection.pub topic, Time.now.to_f.to_s
      @consumer = new_consumer nsqd: nsqd.address, nsqlookupd: nil
      expect(@consumer.pop.timestamp.is_a?(Time)).to eq(true)
    end


    it 'should be when the message was produced, not when it was received by the consumer' do
      connection.pub topic, Time.now.to_f.to_s

      # wait a tick (so the time the consumer receives the message will be different than when it was published)
      sleep 0.1

      @consumer = new_consumer nsqd: nsqd.address, nsqlookupd: nil
      msg       = @consumer.pop
      expect(msg.timestamp.to_f).to be_within(0.01).of(msg.body.to_f)
    end
  end
end
