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
require 'nsq-cluster'

# Show lots more information about the cluster
# ENV['VERBOSE']       = 'true'

# NsqCluster : Set uncommon port numbers to avoid clashing with local instances
Nsqd.base_port       = 24150
Nsqlookupd.base_port = 24160
Nsqadmin.base_port   = 24171

Dir["#{File.dirname(__FILE__)}/support/**/*.rb"].each { |f| require f }

RSpec.configure do |config|
  config.before(:suite) do
    Nsq.logger = Logger.new(STDOUT) if ENV['VERBOSE']
  end
end
