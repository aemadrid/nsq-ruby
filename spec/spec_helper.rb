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

RSpec.configure do |config|
  config.before(:suite) do
    Nsq.logger = Logger.new(STDOUT) if ENV['VERBOSE']
  end

  config.before(:each, cluster: true) do
    @cluster = Nsq::Cluster.new @cluster_options || {}
    @cluster.run!
  end

  config.after(:each, cluster: true) do
    @cluster.halt!
  end
end
