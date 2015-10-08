require 'thread'
require 'concurrent'
require 'concurrent/edge/lock_free_stack'

module Nsq

  class BasicQueue < ::Queue
  end

  class LockFreeQueue < Concurrent::Edge::LockFreeStack

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

  end

  extend self

  def queue_class
    @queue_class || ::Queue
  end

  def queue_class=(value)
    @queue_class = value
  end

end
