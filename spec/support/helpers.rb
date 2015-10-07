module SharedHelperContext
  extend RSpec::Core::SharedContext

  let(:cluster_options) { { nsqds: 2, lookupds: 1 } }

  def cluster
    @cluster ||= Nsq::Cluster.new cluster_options
  end

  let(:nsqd) { cluster.nsqds.first }
  let(:nsqd_url) { nsqd.address :tcp }
  let(:lookupd) { cluster.lookupds.first }
  let(:lookupd_url) { lookupd.address :tcp }
  let(:host) { nsqd.host }
  let(:tcp_port) { nsqd.tcp_port }
  let(:http_port) { nsqd.http_port }

  let(:topic) { 'some-topic' }
  let(:channel) { 'some-channel' }

  let(:consumer_options) { { nsqlookupd: nil, nsqd: nsqd_url, max_in_flight: 10 } }

  def consumer
    @consumer ||= new_consumer consumer_options
  end

  let(:producer_options) { { nsqlookupd: nil, nsqd: nsqd_url, max_in_flight: 10 } }
  let(:producer) { @producer = new_producer producer_options }

  let(:http_producer_options) { { host: host, port: http_port, topic: topic } }
  let(:http_producer) { @http_producer = Nsq::HttpProducer.new http_producer_options }

  let(:connection_options) { { host: host, port: tcp_port } }

  def connection
    @connection ||= Nsq::Connection.new connection_options
  end

  def new_consumer(opts = {})
    lookupd = cluster.lookupds.map { |l| "#{l.host}:#{l.http_port}" }
    options = { topic: topic, channel: channel, nsqlookupd: lookupd, max_in_flight: 1 }.merge(opts)
    Nsq::Consumer.new options
  end

  def new_nsqd_consumer
    new_consumer nsqd: nsqd.address(:tcp), nsqlookupd: nil
  end

  def new_producer(opts = {})
    options = { topic: topic, nsqd: nsqd_url, discovery_interval: 1 }.merge(opts)
    Nsq::Producer.new options
  end

  def new_lookupd_producer(opts = {})
    lookupd = cluster.lookupds.map { |l| l.address :http }
    options = { topic: topic, nsqlookupd: lookupd, discovery_interval: 1 }.merge(opts)
    Nsq::Producer.new options
  end

  def new_discovery(*cluster_lookupds)
    lookupds = cluster_lookupds.flatten.map { |l| l.address :http }
    lookupds.last.prepend 'http://' # one lookupd has scheme and one does not
    Nsq::Discovery.new lookupds
  end

  def message_count
    stats  = http_producer.stats
    topics = stats.data.topics
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
  def wait_for(timeout = 5, msg = 'process', interval = 0.25, &block)
    res        = nil
    start_time = Time.now
    deadline   = start_time + timeout

    while Time.now < deadline
      res = yield
      break if res
      sleep interval
    end
    end_time = Time.now
    took     = end_time - start_time

    Nsq.logger.debug 'Took %is (%is max)%s...' % [took, timeout.to_f, (msg ? " to #{msg}" : '')]
    expect(took).to be <= timeout, "expected for #{msg} to take less than #{timeout}s but took #{took}s"
    [took, res]
  end

  def bnr(msg, chr = '-')
    puts " [ #{msg} ] ".center(120, chr)
  end

end

RSpec.configure do |config|
  config.include SharedHelperContext
end
