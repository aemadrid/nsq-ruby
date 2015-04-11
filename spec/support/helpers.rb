module SharedHelperContext
  extend RSpec::Core::SharedContext

  let(:topic) { 'some-topic' }
  let(:channel) { 'some-channel' }

  let(:cluster_options) { { nsqd_count: 2, nsqlookupd_count: 1 } }
  let!(:cluster) { @cluster = NsqCluster.new cluster_options }

  let(:nsqd) { cluster.nsqd.first }
  let(:nsqd_url) { "#{nsqd.host}:#{nsqd.tcp_port}" }

  let(:consumer_options) { { nsqlookupd: nil, nsqd: nsqd_url, max_in_flight: 10 } }
  let(:consumer) { @consumer = new_consumer consumer_options }

  let(:producer_options) { { nsqlookupd: nil, nsqd: nsqd_url, max_in_flight: 10 } }
  let(:producer) { @producer = new_producer producer_options }

  let(:connection_options) { { host: cluster.nsqd[0].host, port: cluster.nsqd[0].tcp_port } }
  let(:connection) { Nsq::Connection.new connection_options }

  after(:each) do
    @producer.terminate if @producer
    @consumer.terminate if @consumer
    @connection.close if @connection
    @cluster.destroy if @cluster
  end

  def new_consumer(opts = {})
    lookupd = cluster.nsqlookupd.map { |l| "#{l.host}:#{l.http_port}" }
    options = { topic: topic, channel: channel, nsqlookupd: lookupd, max_in_flight: 1 }.merge(opts)
    Nsq::Consumer.new options
  end

  def new_nsqd_consumer
    new_consumer nsqd: nsqd_url, nsqlookupd: nil
  end

  def new_producer(opts = {})
    options = { topic: topic, nsqd: nsqd_url, discovery_interval: 1 }.merge(opts)
    Nsq::Producer.new options
  end

  def new_lookupd_producer(opts = {})
    lookupd = cluster.nsqlookupd.map { |l| "#{l.host}:#{l.http_port}" }
    options = { topic: topic, nsqlookupd: lookupd, discovery_interval: 1 }.merge(opts)
    Nsq::Producer.new options
  end

  def message_count
    topics = JSON.parse(nsqd.stats.body)['data']['topics']
    topic  = topics.select { |t| t['topic_name'] == producer.topic }.first
    topic ? topic['message_count'] : 0
  end

  # This is for certain spots where we're testing connections going up and down.
  # Don't want these tests to take forever to run!
  def set_speedy_connection_timeouts!
    allow_any_instance_of(Nsq::Connection).to receive(:snooze).and_return(nil)
  end

  def assert_no_timeout(time = 1, &block)
    expect {
      Timeout::timeout(time) do
        yield
      end
    }.not_to raise_error
  end

  def assert_timeout(time = 1, &block)
    expect {
      Timeout::timeout(time) do
        yield
      end
    }.to raise_error(Timeout::Error)
  end

  # Block execution until a condition is met
  # Times out after 5 seconds by default
  #
  # example:
  #   wait_for { @consumer.queue.length > 0 }
  #
  def wait_for(timeout = 5, &block)
    Timeout::timeout(timeout) do
      loop do
        break if yield
        sleep(0.1)
      end
    end
  end

end

RSpec.configure do |config|
  config.include SharedHelperContext
end
