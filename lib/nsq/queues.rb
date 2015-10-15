require 'thread'
require 'concurrent'
require 'concurrent/edge/lock_free_stack'

module Nsq
  module Queues

    class Basic < ::Queue
    end

    class LockFree < Concurrent::Edge::LockFreeStack

      def initialize
        super
        @mutex = ::Mutex.new
      end

      alias :<< :push

      def pop(non_block = false)
        if non_block
          res = super()
          raise ThreadError.new('queue empty') if res.nil?
        else
          loop do
            res = super()
            break unless res.nil?
            sleep 0.1
          end
        end
        res
      end

      # This method is very unsafe and should only be used in _safe_ (testing?) environments to keep the same API as ::Queue
      def size
        return 0 if empty?
        @mutex.synchronize do
          found = []
          found << pop(true) until empty?
          final = found.size
          found.each { |x| push x }
          final
        end
      end

    end
  end

  extend self

  def queue_class
    # @queue_class || ::Queue
    @queue_class || Queues::Basic
    # @queue_class || Queues::LockFree
  end

  def queue_class=(value)
    @queue_class = value
  end

end
