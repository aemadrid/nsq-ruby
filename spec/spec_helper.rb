# -*- encoding: utf-8 -*-
require 'rubygems'
require 'bundler'

begin
  Bundler.setup(:default, :development)
rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts 'Run `bundle install` to install missing gems'
  exit e.status_code
end

$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))

require 'timeout'
require 'rspec'
require 'nsq'
require 'nsq-cluster'

# Set uncommon port numbers to avoid clashing with local instances
Nsqd.base_port       = 24150
Nsqlookupd.base_port = 24160
Nsqadmin.base_port   = 24171

Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].each { |f| require f }

RSpec.configure do |config|
  config.before(:suite) do
    Nsq.logger = Logger.new(STDOUT) if ENV['VERBOSE']
  end
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

TOPIC   = 'some-topic'
CHANNEL = 'some-channel'

def new_consumer(opts = {})
  lookupd = @cluster.nsqlookupd.map { |l| "#{l.host}:#{l.http_port}" }
  Nsq::Consumer.new({
                      topic:         TOPIC,
                      channel:       CHANNEL,
                      nsqlookupd:    lookupd,
                      max_in_flight: 1
                    }.merge(opts))
end

def new_producer(nsqd, opts = {})
  Nsq::Producer.new({
                      topic:              TOPIC,
                      nsqd:               "#{nsqd.host}:#{nsqd.tcp_port}",
                      discovery_interval: 1
                    }.merge(opts))
end

def new_lookupd_producer(opts = {})
  lookupd = @cluster.nsqlookupd.map { |l| "#{l.host}:#{l.http_port}" }
  Nsq::Producer.new({
                      topic:              TOPIC,
                      nsqlookupd:         lookupd,
                      discovery_interval: 1
                    }.merge(opts))
end

# This is for certain spots where we're testing connections going up and down.
# Don't want these tests to take forever to run!
def set_speedy_connection_timeouts!
  allow_any_instance_of(Nsq::Connection).to receive(:snooze).and_return(nil)
end
