module SharedHelperContext
  extend RSpec::Core::SharedContext

  let(:cluster_options) { { nsqds: 2, lookupds: 1 } }

  def cluster
    return @cluster if @cluster
    puts "cluster_options (#{cluster_options.class.name}) #{cluster_options.inspect}"
    @cluster = Nsq::Cluster.new cluster_options
    puts "@cluster (#{@cluster.class.name}) #{@cluster.inspect}"
    @cluster
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
    return @consumer if @consumer
    puts "consumer_options (#{consumer_options.class.name}) #{consumer_options.inspect}"
    @consumer = new_consumer consumer_options
    puts "@consumer (#{@consumer.class.name}) #{@consumer.inspect}"
    @consumer
  end

  let(:producer_options) { { nsqlookupd: nil, nsqd: nsqd_url, max_in_flight: 10 } }
  let(:producer) { @producer = new_producer producer_options }

  let(:http_producer_options) { { host: host, port: http_port, topic: topic } }
  let(:http_producer) { @http_producer = Nsq::HttpProducer.new http_producer_options }

  let(:connection_options) { { host: host, port: tcp_port } }

  def connection
    return @connection if @connection
    puts "connection_options (#{connection_options.class.name}) #{connection_options.inspect}"
    @connection = Nsq::Connection.new connection_options
    puts "@@connection (#{@connection.class.name}) #{@connection.inspect}"
    @connection
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
    puts "options : (#{options.class.name}) #{options.to_yaml}"
    Nsq::Producer.new options
  end

  def message_count
    stats   = http_producer.stats
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
  def wait_for(timeout = 5, &block)
    Timeout::timeout(timeout) do
      loop do
        break if yield
        sleep(0.1)
      end
    end
  end

  def bnr(msg, chr = '-')
    puts " [ #{msg} ] ".center(120, chr)
  end

end

RSpec.configure do |config|
  config.include SharedHelperContext
end
