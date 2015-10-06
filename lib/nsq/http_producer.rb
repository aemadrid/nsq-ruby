require 'http'
require 'uri'
require 'ostruct'
require 'cgi'
require 'multi_json'

module Nsq
  class HttpProducer

    class Response

      attr_accessor :status_code, :status_txt, :response, :data
      alias :code :status_code
      alias :status :status_txt

      def initialize(hsh)
        hsh.each { |k, v| send "#{k}=", v }
      end

      def data=(value)
        case value
          when Hash
            @data = build_data value
          else
            @data = value
        end
      end

      def success?
        status.to_s == 'OK'
      end

      private

      def build_data(hsh, klass = OpenStruct)
        klass.new.tap do |resp|
          hsh.each do |key, value|
            resp[key] = value.is_a?(Hash) ? build_data(value, klass) : value
          end
        end
      end

    end

    attr_reader :uri
    attr_accessor :secure, :host, :port, :topic, :config

    def initialize(args = {})
      args = { secure: false, host: '0.0.0.0', port: 4161, config: {} }.update args
      args.each { |k, v| send "#{k}=", v }
      @uri = URI.parse "http#{secure ? 's' : ''}://#{host}:#{port}"
    end

    def write(*payload)
      if payload.size == 1
        payload = payload.first
        send_message :post, :pub, body: payload, params: { topic: topic }
      else
        send_message :post, :mpub, body: payload.join("\n"), params: { topic: topic }
      end
    end

    def create_topic
      send_message :post, :create_topic, params: { topic: topic }
    end

    def delete_topic
      send_message :post, :delete_topic, params: { topic: topic }
    end

    def create_channel(chan)
      send_message :post, :create_channel, params: { topic: topic, channel: chan }
    end

    def delete_channel(chan)
      send_message :post, :delete_channel, params: { topic: topic, channel: chan }
    end

    def empty_topic
      send_message :post, :empty_topic, params: { topic: topic }
    end

    def empty_channel(chan)
      send_message :post, :empty_channel, params: { topic: topic, channel: chan }
    end

    def pause_channel(chan)
      send_message :post, :pause_channel, params: { topic: topic, channel: chan }
    end

    def unpause_channel(chan)
      send_message :post, :unpause_channel, params: { topic: topic, channel: chan }
    end

    def stats(format = 'json')
      send_message :get, :stats, params: { format: format }
    end

    def ping
      send_message :get, :ping
    end

    def info
      send_message :get, :info
    end

    private

    def send_message(method, path, args = {})
      build      = uri.dup
      build.path = "/#{path}"
      begin
        response   = HTTP.send method, build.to_s, args.merge(config)
        hsh = MultiJson.load response.body.to_s
      rescue Errno::ECONNREFUSED
        hsh = {
          'status_code' => '500',
          'status_txt'  => 'Errno::ECONNREFUSED',
          'response'    => 'Errno::ECONNREFUSED',
          'data'        => nil,
        }
      rescue MultiJson::LoadError
        hsh = {
          'status_code' => response.code,
          'status_txt'  => response.body.to_s,
          'response'    => response.body.to_s,
          'data'        => nil,
        }
      end
      Response.new hsh
    end

  end
end
