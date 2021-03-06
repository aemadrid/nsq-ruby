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
    res        = false
    start_time = Time.now
    deadline   = start_time + timeout

    while Time.now < deadline
      res = yield
      break if res
      sleep interval
    end
    end_time = Time.now
    took     = end_time - start_time

    fail "did not return a value in ##{timeout}s for #{msg}" unless res
    puts "we found what we were looking for in #{msg}"
    [took, res]
  end

  def bnr(msg, chr = '-')
    puts " [ #{msg} ] ".center(120, chr)
  end

  shared_examples 'a thread-safe queue' do
    let(:msg) { 'some message' }
    let!(:queue) { described_class.new }
    let!(:ary) { Concurrent::Array.new }

    context 'basic' do
      it 'initialized without throwing an error' do
        expect { described_class.new }.to_not raise_error
      end
      it 'throws an exception on empty non_block pop' do
        expect { queue.pop(true) }.to raise_error ThreadError, 'queue empty'
      end
      it 'blocks on empty blocking pop' do
        ary = Concurrent::Array.new
        thr = Thread.new { ary << queue.pop(false) }
        expect(ary.size).to eq 0

        queue.push msg
        thr.join

        expect(ary.size).to eq 1
      end
      it 'responds to empty?/push/pop' do
        expect(queue.empty?).to be_truthy
        queue.push 1
        expect(queue.empty?).to be_falsey
        res = queue.pop
        expect(res).to eq 1
        expect(queue.empty?).to be_truthy
      end
      it 'responds to size/push/pop' do
        expect(queue.size).to eq 0
        queue.push 1
        expect(queue.size).to eq 1
        res = queue.pop
        expect(res).to eq 1
        expect(queue.size).to eq 0
      end
    end
    context 'with 2 threads' do
      let(:qty) { 100_000 }
      let(:exp) { qty.times.map { |x| 'msg%05i' % [x + 1] } }
      let(:extra1) { ary.sort - exp }
      let(:extra2) { exp - ary.sort }
      it 'can push and pop' do
        thr_read_and_write exp, queue, ary

        expect(ary.size).to eq qty
        expect(extra1).to eq []
        expect(extra2).to eq []
      end
    end
  end

  def thr_write(rows, q, a, qty = rows.size, offset = 0)
    Thread.new do
      rows[offset, qty].each do |row|
        q.push row
        # Nsq.logger.debug '%-20.20s | %-10.10s | %5i | %s' % [described_class.name, 'writer', a.size, row.to_s]
      end
    end
  end

  def thr_read(qty, q, a, non_block = false)
    Thread.new do
      qty.times do
        row = q.pop non_block
        a << row
        # Nsq.logger.debug '%-20.20s | %-10.10s | %5i | %s' % [described_class.name, 'reader', a.size, row.to_s]
      end
    end
  end

  def thr_read_and_write(exp, q, a, non_block = false)
    start_time = Time.now
    threads    = []
    threads << thr_write(exp, q, a)
    threads << thr_read(exp.size, q, a, non_block)
    threads.map { |x| x.join }
    end_time  = Time.now
    took_time = end_time - start_time
    ms        = (exp.size / took_time / 60.0).to_i
    puts '%-20.20s | Took %.2fs to process %i messages (%imsg/m)' % [described_class, took_time, exp.size, ms]
  end

  shared_examples 'a basic successful http producer response' do
    it('type    ') { expect(res).to be_a Nsq::HttpProducer::Response }
    it('code    ') { expect(res.code).to eq 200 }
    it('status  ') { expect(res.status).to eq 'OK' }
    it('response') { expect(res.response).to eq 'OK' }
    it('data    ') { expect(res.data).to be_nil }
    it('success?') { expect(res.success?).to be_truthy }
  end

  shared_examples 'a simple successful http producer response' do
    it('type    ') { expect(res).to be_a Nsq::HttpProducer::Response }
    it('code    ') { expect(res.code).to eq 200 }
    it('status  ') { expect(res.status).to eq 'OK' }
    it('success?') { expect(res.success?).to be_truthy }
  end

end

RSpec.configure do |config|
  config.include SharedHelperContext
end
