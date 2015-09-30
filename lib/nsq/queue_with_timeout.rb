# From http://spin.atomicobject.com/2014/07/07/ruby-queue-pop-timeout/

require 'thread'
require 'timeout'

module Nsq
  class QueueWithTimeout

    def initialize
      @mutex    = Mutex.new
      @queue    = []
      @recieved = ConditionVariable.new
    end

    def push(x)
      @mutex.synchronize do
        @queue << x
        @recieved.signal
      end
    end

    alias enq push
    alias << push

    def pop(non_block = false)
      pop_with_timeout(non_block ? 0 : nil)
    end

    alias deq pop
    alias shift pop

    def pop_with_timeout(timeout = nil)
      @mutex.synchronize do
        if @queue.empty?
          @recieved.wait(@mutex, timeout) if timeout != 0
          # if we're still empty after the timeout, raise exception
          raise Timeout::Error, 'empty queue' if @queue.empty?
        end
        @queue.shift
      end
    end

    def size
      @mutex.synchronize { @queue.size }
    end

    alias length size

    def empty?
      size == 0
    end

  end
end