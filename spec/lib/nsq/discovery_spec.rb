# -*- encoding: utf-8 -*-
require_relative '../../spec_helper'

describe Nsq::Discovery do

  let(:nsqd_count) { 5 }

  let(:topic) { 'some-topic' }
  let!(:cluster) do
    NsqCluster.new(nsqd_count: nsqd_count, nsqlookupd_count: 2).tap do |c|
      c.nsqd.take(nsqd_count - 1).each { |nsqd| nsqd.pub(topic, 'some-message') }
      c.nsqd.last.pub('some-other-topic', 'some-message')
    end
  end
  let(:expected_topic_lookup_nsqds) { cluster.nsqd.take(nsqd_count - 1).map { |d| "#{d.host}:#{d.tcp_port}" }.sort }
  let(:expected_all_nsqds) { cluster.nsqd.map { |d| "#{d.host}:#{d.tcp_port}" }.sort }

  after(:each) do
    cluster.destroy
  end

  def new_discovery(cluster_lookupds)
    lookupds = cluster_lookupds.map { |lookupd| "#{lookupd.host}:#{lookupd.http_port}" }

    # one lookupd has scheme and one does not
    lookupds.last.prepend 'http://'

    Nsq::Discovery.new lookupds
  end

  describe 'a single nsqlookupd' do
    let(:discovery) { new_discovery [cluster.nsqlookupd.first] }

    describe '#nsqds' do
      it 'returns all nsqds' do
        nsqds = discovery.nsqds.sort
        expect(nsqds).to eq(expected_all_nsqds)
      end
    end

    describe '#nsqds_for_topic' do
      it 'returns [] for a topic that doesn\'t exist' do
        nsqds = discovery.nsqds_for_topic('topic-that-does-not-exists')
        expect(nsqds).to eq([])
      end

      it 'returns all nsqds' do
        nsqds = discovery.nsqds_for_topic(topic)
        expect(nsqds.sort).to eq(expected_topic_lookup_nsqds)
      end
    end
  end


  describe 'multiple nsqlookupds' do
    let(:discovery) { new_discovery cluster.nsqlookupd }

    describe '#nsqds_for_topic' do
      it 'returns all nsqds' do
        nsqds = discovery.nsqds_for_topic(topic)
        expect(nsqds.sort).to eq(expected_topic_lookup_nsqds)
      end
    end
  end


  describe 'multiple nsqlookupds, but one is down' do
    let(:discovery) { new_discovery cluster.nsqlookupd }
    let!(:downed_nsqlookupd) { cluster.nsqlookupd.first.tap { |x| x.stop } }

    describe '#nsqds_for_topic' do
      it 'returns all nsqds' do
        nsqds = discovery.nsqds_for_topic(topic)
        expect(nsqds.sort).to eq(expected_topic_lookup_nsqds)
      end
    end
  end


  describe 'when all lookupds are down' do
    let(:discovery) { new_discovery cluster.nsqlookupd }
    before(:each) do
      cluster.nsqlookupd.each(&:stop)
    end

    describe '#nsqds' do
      it 'throws an exception' do
        expect { discovery.nsqds }.to raise_error(Nsq::DiscoveryException)
      end
    end

    describe '#nsqds_for_topic' do
      it 'throws an exception' do
        expect { discovery.nsqds_for_topic(topic) }.to raise_error(Nsq::DiscoveryException)
      end
    end
  end
end
