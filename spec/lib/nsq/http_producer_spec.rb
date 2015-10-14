describe Nsq::HttpProducer do
  context 'connecting directly to a single nsqd' do
    let(:topic) { 'some-other-topic' }
    let(:channel) { 'some-other-channel' }
    let(:cluster_options) { { nsqds: 1 } }
    subject { Nsq::HttpProducer.new http_producer_options }
    before(:each) { subject.create_topic }
    after(:each) { subject.delete_topic }
    context 'write' do
      context 'a single message' do
        let(:res) { subject.write 'testing' }
        it_behaves_like 'a basic successful http producer response'
      end
      context 'multiple messages' do
        let(:res) { subject.write 'testing1', 'testing2', 'testing3' }
        it_behaves_like 'a basic successful http producer response'
      end
    end
    context 'topics' do
      context 'create' do
        let(:res) { subject.create_topic }
        it_behaves_like 'a simple successful http producer response'
        it('response') { expect(res.response).to eq nil }
      end
      context 'empty' do
        let(:res) { subject.empty_topic }
        it_behaves_like 'a simple successful http producer response'
        it('response') { expect(res.response).to eq nil }
      end
      context 'delete' do
        let(:res) { subject.delete_topic }
        it_behaves_like 'a simple successful http producer response'
        it('response') { expect(res.response).to eq nil }
      end
    end

    context 'channels' do
      before(:each) { subject.create_channel(channel) }
      context 'create' do
        let(:res) { subject.create_channel(channel) }
        it_behaves_like 'a simple successful http producer response'
        it('response') { expect(res.response).to eq nil }
      end
      context 'empty' do
        let(:res) { subject.empty_channel(channel) }
        it_behaves_like 'a simple successful http producer response'
        it('response') { expect(res.response).to eq nil }
      end
      context 'delete' do
        let(:res) { subject.delete_channel(channel) }
        it_behaves_like 'a simple successful http producer response'
        it('response') { expect(res.response).to eq nil }
      end
    end
    context 'stats' do
      before(:each) { subject.create_channel(channel); subject.write 't1', 't2' }
      after(:each) { subject.delete_channel(channel) }
      let(:res) { subject.stats }
      let(:topic_stats){ subject.topic_stats }
      let(:channel_stats){ subject.channel_stats channel }
      it_behaves_like 'a simple successful http producer response'
      it('type          ') { expect(res.data).to be_a OpenStruct }
      it('health        ') { expect(res.data.health).to eq 'OK' }
      it('topics        ') { expect(res.data.topics.size).to eq 1 }
      it('topics        ') { expect(res.data.topics).to be_a Array }
      it('topic name    ') { expect(res.data.topics.first['topic_name']).to eq topic }
      it('topic channels') { expect(res.data.topics.first['channels'].size).to eq 1 }
      it('topic channel ') { expect(res.data.topics.first['channels'].first['channel_name']).to eq channel }
      it('topic stats type  ') { expect(topic_stats).to be_a OpenStruct }
      it('topic stats name  ') { expect(topic_stats.topic_name).to eq topic }
      it('topic stats channels') { expect(topic_stats.channels.size).to eq 1 }
      it('topic stats count   ') { expect(topic_stats.message_count).to eq 2 }
      it('channel stats type') { expect(channel_stats).to be_a OpenStruct }
      it('channel stats name  ') { expect(channel_stats.channel_name).to eq channel }
      it('channel stats count   ') { expect(channel_stats.message_count).to eq 2 }
    end
    context 'ping' do
      let(:res) { subject.ping }
      it_behaves_like 'a simple successful http producer response'
      it('type') { expect(res.data).to be_nil }
    end
    context 'info' do
      let(:res) { subject.info }
      it_behaves_like 'a simple successful http producer response'
      it('type   ') { expect(res.data).to be_a OpenStruct }
      it('version') { expect(res.data.version).to match /0\.\d+\.\d+/ }
    end
  end
end
