describe Nsq::Discovery do
  let(:nsqd_count) { 5 }
  let(:topic) { 'some-topic' }
  let(:cluster_options) { { nsqds: nsqd_count, lookupds: 2 } }
  let(:expected_topic_lookup_nsqds) { cluster.nsqds.take(nsqd_count - 1).map { |n| n.address(:tcp) }.sort }
  let(:expected_all_nsqds) { cluster.nsqds.map { |n| n.address(:tcp) }.sort }
  let(:discovery) { new_discovery cluster.lookupds }

  before(:each) do
    cluster.nsqds.each_with_index do |n, idx|
      name = idx < (nsqd_count - 1) ? topic : 'some-other-topic'
      n.conn { |x| x.pub name, 'some-message' }
    end
  end

  describe 'a single nsqlookupd' do
    let(:discovery) { new_discovery cluster.lookupds.first }
    describe '#nsqds' do
      it 'returns all nsqds' do
        nsqds = discovery.nsqds.sort
        expect(nsqds).to eq(expected_all_nsqds)
      end
    end
    describe '#nsqds_for_topic' do
      it 'returns [] for a topic that doesn\'t exist' do
        nsqds = discovery.nsqds_for_topic 'topic-that-does-not-exists'
        expect(nsqds).to eq []
      end
      it 'returns all nsqds' do
        nsqds = discovery.nsqds_for_topic topic
        expect(nsqds.sort).to eq expected_topic_lookup_nsqds
      end
    end
  end
  describe 'multiple nsqlookupds' do
    describe '#nsqds_for_topic' do
      it 'returns all nsqds' do
        nsqds = discovery.nsqds_for_topic topic
        expect(nsqds.sort).to eq expected_topic_lookup_nsqds
      end
    end
  end
  describe 'multiple nsqlookupds, but one is down' do
    let!(:downed_nsqlookupd) { cluster.lookupds.first.stop }
    describe '#nsqds_for_topic' do
      it 'returns all nsqds' do
        nsqds = discovery.nsqds_for_topic topic
        expect(nsqds.sort).to eq expected_topic_lookup_nsqds
      end
    end
  end
  describe 'when all lookupds are down' do
    before(:each) do
      cluster.lookupds.each(&:stop)
    end
    describe '#nsqds' do
      it 'throws an exception' do
        expect { discovery.nsqds }.to raise_error Nsq::DiscoveryException
      end
    end
    describe '#nsqds_for_topic' do
      it 'throws an exception' do
        expect { discovery.nsqds_for_topic topic }.to raise_error Nsq::DiscoveryException
      end
    end
  end
end
