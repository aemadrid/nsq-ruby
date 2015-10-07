require 'childprocess'
require 'socket'

ChildProcess.posix_spawn = true

require 'tmpdir'
require 'fileutils'
require 'yaml'
require 'forwardable'

module Nsq
  class Cluster

    include Nsq::AttributeLogger

    class DaemonManager

      extend Forwardable
      include Comparable

      attr_reader :host, :tcp_port, :http_port, :tmp_dir, :process, :options, :commands

      alias :port :tcp_port

      def process
        @process ||= make_process
      end

      def running?
        !@process.nil? && @process.alive?
      end

      def start
        return false if running?
        process.start
        wait
      end

      def wait
        check :http
      end

      def stop
        return false unless running?
        process.stop
        process.wait
        @process = nil
      end

      def address(type = :tcp)
        "#{host}:#{send("#{type}_port")}"
      end

      def check(type)
        TCPSocket.new host, send("#{type}_port")
      rescue Errno::ECONNREFUSED
        retry
      end

      def to_s
        "#<#{self.class.name} " +
          "host=#{host.inspect} " +
          "tcp_port=#{tcp_port} " +
          "http_port=#{http_port} " +
          "running=#{running?} " +
          "pid=#{process ? process.pid : ''}>"
      end

      alias :inspect :to_s

      def <=>(other)
        to_s <=> other.to_s
      end

      private

      def make_process
        ChildProcess.build(*commands).tap do |process|
          process.io.inherit! if ENV['VERBOSE'] == 'true'
          process.cwd = tmp_dir
        end
      end

    end

    class NsqAdminManager < DaemonManager

      attr_reader :lookupds

      def initialize(host, http_port, lookupds, options = {})
        @host      = host
        @tcp_port  = tcp_port
        @http_port = http_port
        @tmp_dir   = tmp_dir
        @lookupds  = lookupds
        @options   = build_options options
        @commands  = build_commands
        @process   = make_process
      end

      private

      def build_options(extra_options)
        {
          'http-address' => address(:http),
        }.merge extra_options
      end

      def build_commands
        ['nsqadmin'] + options.map { |k, v| ["-#{k}", v] }.tap do |cmds|
          lookupds.each { |addr| cmds << '-lookupd-http-address' << addr }
        end.flatten
      end
    end

    class NsqLookupdManager < DaemonManager

      def initialize(host, tcp_port, http_port, tmp_dir, options = {})
        @host      = host
        @tcp_port  = tcp_port
        @http_port = http_port
        @tmp_dir   = tmp_dir
        @options   = build_options options
        @commands  = build_commands
        @process   = make_process
      end

      private

      def build_options(extra_options)
        {
          'tcp-address'  => address(:tcp),
          'http-address' => address(:http),
        }.merge extra_options
      end

      def build_commands
        (['nsqlookupd'] + options.map { |k, v| ["-#{k}", v] }).flatten
      end
    end

    class NsqdManager < DaemonManager

      extend Forwardable

      attr_reader :tmp_dir, :lookupds

      def initialize(host, tcp_port, http_port, tmp_dir, lookupds, options = {})
        @host      = host
        @tcp_port  = tcp_port
        @http_port = http_port
        @tmp_dir   = tmp_dir
        @lookupds  = lookupds
        @options   = build_options options
        @commands  = build_commands
        @process   = make_process
      end

      def wait
        check :tcp
      end

      def conn
        Nsq::Connection.new(host: host, port: tcp_port).tap do |konn|
          if block_given?
            res = yield konn
            konn.close
            res
          else
            konn
          end
        end
      end

      private

      def build_options(extra_options)
        {
          'data-path'    => tmp_dir,
          'tcp-address'  => address(:tcp),
          'http-address' => address(:http),
        }.merge extra_options
      end

      def build_commands
        ['nsqd'] + options.map { |k, v| ["-#{k}", v] }.tap do |cmds|
          lookupds.each { |addr| cmds << '-lookupd-tcp-address' << addr }
        end.flatten
      end
    end

    class << self

      attr_reader :port

      def ports(n=1)
        @port  ||= 9_000 + rand(3_000)
        result = n.times.inject([]) { |m, _| m.push(@port += 1) }
        n == 1 ? result.first : result
      end

    end

    attr_reader :args, :host, :nsqds, :lookupds, :admins

    def initialize(options = {})
      @args    = {
        lookupds:        1,
        lookupd_options: {},
        nsqds:           1,
        nsqd_options:    {},
        admins:          0,
        admin_options:   {},
        host:            Socket.gethostname,
      }.update options
      @host    = @args[:host]
      @tmp_dir = Dir.mktmpdir "nsq-ruby-#{self.class.port}"
      build_lookupds
      build_nsqds
      build_admins
      at_exit { halt! }
    end

    def all
      [lookupds, nsqds, admins].flatten.sort
    end

    def running?
      return false if all.empty?
      all.all? { |x| x.running? }
    end

    def run!
      FileUtils.mkdir_p tmp_dir unless File.directory?(tmp_dir)
      nsqds.map { |x| x.start }
      lookupds.map { |x| x.start }
      admins.map { |x| x.start }
      nsqds.each { |x| x.check :tcp }
      all
    end

    def halt!
      nsqds.map { |x| x.stop }
      lookupds.map { |x| x.stop }
      admins.map { |x| x.stop }
      FileUtils.rm_rf tmp_dir if File.directory?(tmp_dir)
      all
    end

    def nsqd
      nsqds.first
    end

    def lookupd
      lookupds.first
    end

    def nsqd_for_idx(idx)
      nsqds[idx % nsqds.length]
    end

    def to_s
      %{#<#{self.class.name} host=#{host} } +
        %{admins=#{admins_running_count}/#{args[:admins]} } +
        %{lookupds=#{lookupds_running_count}/#{args[:lookupds]} } +
        %{nsqds=#{nsqds_running_count}/#{args[:nsqds]}>}
    end

    alias :inspect :to_s

    private

    attr_reader :tmp_dir

    def build_lookupds
      @lookupds = args[:lookupds].times.map do
        ports = self.class.ports(2)
        NsqLookupdManager.new host, ports.first, ports.last, tmp_dir, args[:lookupd_options]
      end
    end

    def build_nsqds
      @nsqds = args[:nsqds].times.map do
        ports = self.class.ports(2)
        NsqdManager.new host, ports.first, ports.last, tmp_dir, lookupds.map { |x| x.address :tcp }, args[:nsqd_options]
      end
    end

    def build_admins
      @admins = args[:admins].times.map do
        port = self.class.ports(1)
        NsqAdminManager.new host, port, lookupds.map { |x| x.address :http }, args[:admin_options]
      end
    end

    def admins_running_count
      return 0 if admins.nil?
      admins.count { |x| x.running? }
    end

    def lookupds_running_count
      return 0 if lookupds.nil?
      lookupds.count { |x| x.running? }
    end

    def nsqds_running_count
      return 0 if nsqds.nil?
      nsqds.count { |x| x.running? }
    end

  end
end
