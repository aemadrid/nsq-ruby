require_relative 'client_base'
require_relative 'queue_with_timeout'

module Nsq
  class Consumer < ClientBase

    attr_reader :max_in_flight

    def initialize(opts = {})
      if opts[:nsqlookupd]
        @nsqlookupds = [opts[:nsqlookupd]].flatten
      else
        @nsqlookupds = []
      end

      @topic   = opts[:topic] || raise(ArgumentError, 'topic is required')
      @channel = opts[:channel] || raise(ArgumentError, 'channel is required')

      raise(ArgumentError, 'invalid topic name') unless valid_topic_name?(@topic)
      raise(ArgumentError, 'invalid channel name') unless valid_channel_name?(@channel)

      @max_in_flight      = opts[:max_in_flight] || 1
      @discovery_interval = opts[:discovery_interval] || 60
      @msg_timeout        = opts[:msg_timeout]

      # This is where we queue up the messages we receive from each connection
      @messages           = opts[:queue] || Nsq::QueueWithTimeout.new

      # This is where we keep a record of our active nsqd connections
      # The key is a string with the host and port of the instance (e.g.
      # '127.0.0.1:4150') and the key is the Connection instance.
      @connections        = {}

      if opts[:nsqlookupd]
        nsqlookupds = [opts[:nsqlookupd]].flatten
        discover_repeatedly(
          nsqlookupds: nsqlookupds,
          topic:       @topic,
          interval:    @discovery_interval
        )

      elsif opts[:nsqd]
        nsqds = [opts[:nsqd]].flatten
        nsqds.each { |d| add_connection(d, max_in_flight: @max_in_flight) }

      else
        add_connection('127.0.0.1:4150')
      end

      at_exit { terminate }
    end

    # pop the next message off the queue
    def pop(timeout = nil)
      if timeout
        @messages.pop_with_timeout timeout
      else
        @messages.pop
      end
    end

    # returns the number of messages we have locally in the queue
    def size
      @messages.size
    end

    private

    def add_connection(nsqd, options = {})
      super(nsqd, {
                  topic:         @topic,
                  channel:       @channel,
                  queue:         @messages,
                  msg_timeout:   @msg_timeout,
                  max_in_flight: 1
                }.merge(options))
    end

    # Be conservative, but don't set a connection's max_in_flight below 1
    def max_in_flight_per_connection(number_of_connections = @connections.length)
      [@max_in_flight / number_of_connections, 1].max
    end

    def connections_changed
      redistribute_ready
    end

    def redistribute_ready
      @connections.values.each do |connection|
        connection.max_in_flight = max_in_flight_per_connection
        connection.re_up_ready
      end
    end
  end
end
