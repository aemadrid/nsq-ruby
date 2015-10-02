require 'childprocess'

ChildProcess.posix_spawn = true

require 'tmpdir'
require 'fileutils'
require 'yaml'

module Nsq
  class Cluster

    class << self

      attr_reader :port

      def ports(n=1)
        @port  ||= 6677
        result = n.times.inject([]) { |m, _| m.push(@port += 1) }
        n == 1 ? result.first : result
      end

    end

    attr_reader :args, :host, :nsqds, :lookupds

    def initialize(options = {})
      puts "initialize | options (#{options.class.name}) #{options.to_yaml}"
      @args    = {
        lookupds:        1,
        lookupd_options: {},
        nsqds:           1,
        nsqd_options:    {},
        host:            '0.0.0.0'
      }.update options
      @host    = @args[:host]
      @tmp_dir = Dir.mktmpdir "nsq-ruby-#{self.class.port}"
      puts "initialize | args (#{args.class.name}) #{args.to_yaml}"
      puts "initialize | host (#{host.class.name}) #{host.inspect}"
      puts "initialize | tmp_dir (#{tmp_dir.class.name}) #{tmp_dir.inspect}"
    end

    def run!
      @lookupds = args[:lookupds].times.map { build_lookupd }
      @nsqds    = args[:nsqds].times.map { build_nsqd }
      nsqd_tcp_addresses.each { |address| check_address address }
    end

    def halt!
      @nsqds.map { |x| x.first }.map { |x| x.stop }
      @lookupds.map { |x| x.first }.map { |x| x.stop }
      @nsqds.map { |x| x.first }.map { |x| x.wait }
      @lookupds.map { |x| x.first }.map { |x| x.wait }
      FileUtils.rm_rf tmp_dir
    end

    def nsqd_tcp_port(meth = :first)
      nsqd_tcp_addresses.send(meth).split(':').last
    end

    def nsqd_http_port(meth = :first)
      nsqd_http_addresses.send(meth).split(':').last
    end

    def lookupd_http_port(meth = :first)
      lookupd_http_addresses.send(meth).split(':').last
    end

    def lookupd_tcp_port(meth = :first)
      lookupd_tcp_addresses.send(meth).split(':').last
    end

    private

    attr_reader :tmp_dir

    def build_address(port)
      "#{host}:#{port}"
    end

    def nsqd_tcp_addresses
      @nsqds.map { |x| x.last }.map { |options| options['tcp-address'] }
    end

    def nsqd_http_addresses
      @nsqds.map { |x| x.last }.map { |options| "http://#{options['http-address']}" }
    end

    def lookupd_http_addresses
      @lookupds.map { |x| x.last }.map { |options| "http://#{options['http-address']}" }
    end

    def lookupd_tcp_addresses
      @lookupds.map { |x| x.last }.map { |options| options['tcp-address'] }
    end

    def build_nsqd
      ports   = self.class.ports(2)
      options = {
        'data-path'    => tmp_dir,
        'tcp-address'  => build_address(ports.first),
        'http-address' => build_address(ports.last),
      }.merge args[:nsqd_options]
      puts "build_nsqd : options (#{options.class.name}) #{options.to_yaml}"
      cmd = ['nsqd'] + options.map { |k, v| ["-#{k}", v] }
      lookupd_tcp_addresses.each { |addr| cmd << '-lookupd-tcp-address' << addr }
      [make_process(cmd), options]
    end

    def build_lookupd
      ports   = self.class.ports(2)
      options = {
        'tcp-address'  => build_address(ports.first),
        'http-address' => build_address(ports.last),
      }.merge args[:lookupd_options]
      cmd     = ['nsqlookupd'] + options.map { |k, v| ["-#{k}", v] }
      res     = [make_process(cmd), options]
      puts "res (#{res.class.name}) #{res.to_yaml}"
      res
    end

    def make_process(*cmds)
      ChildProcess.build(*cmds.flatten).tap do |process|
        process.io.inherit! if ENV['DEBUG']
        process.cwd = tmp_dir
        process.start
      end
    end

    def check_address(address)
      host, port = address.split(':')
      TCPSocket.new host, port
    rescue Errno::ECONNREFUSED
      retry
    end

  end
end
