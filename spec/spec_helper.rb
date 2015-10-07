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
require 'rspec/core/shared_context'
require 'nsq'

# Show lots more information about the cluster
# ENV['VERBOSE']       = 'true'

Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].each { |f| require f }

RSpec.configure do |c|

  c.filter_run focus: true if ENV['FOCUS'] == 'true'
  c.filter_run_excluding skip: true unless ENV['FULL'] == 'true'
  c.filter_run_excluding performance: true unless ENV['PERFORMANCE'] == 'true'

  c.before(:suite) do
    Nsq.logger = Logger.new(STDOUT) if ENV['DEBUG'] = 'true'
  end

  c.before(:each) do
    cluster.run!
  end

  c.after(:each) do
    @connection.close if @connection
    @consumer.terminate if @consumer
    @producer.terminate if @producer
    @cluster.halt! if @cluster
  end

end
