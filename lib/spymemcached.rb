require "java"
require "spymemcached/memcached-2.5.jar"

class Spymemcached
  java_import "net.spy.memcached.MemcachedClient"
  java_import "net.spy.memcached.BinaryConnectionFactory"
  java_import "net.spy.memcached.DefaultConnectionFactory"
  java_import "net.spy.memcached.ops.OperationException"
  java_import "net.spy.memcached.transcoders.Transcoder"
  java_import "net.spy.memcached.CachedData"
  java_import "java.net.InetSocketAddress"
  java_import "java.util.concurrent.TimeUnit"

  class RubyTranscoder
    include Transcoder

    def asyncDecode(data)
      false
    end

    def decode(data)
      Marshal.load(String.from_java_bytes(data.getData))
    end

    def encode(obj)
      CachedData.new(0, Marshal.dump(obj).to_java_bytes, getMaxSize)
    end

    def getMaxSize
      CachedData::MAX_SIZE
    end
  end


  def initialize(servers, options = {}, transcoder = RubyTranscoder.new)
    @transcoder = transcoder
    @options = options

    conn_factory = if options[:binary_protocol]
                     BinaryConnectionFactory.new
                   else
                     DefaultConnectionFactory.new
                   end
    @servers = servers
    @servers_in_java = servers.map do |s|
      host, port = s.split(":")
      InetSocketAddress.new(host, port.to_i)
    end
    @client     = MemcachedClient.new(@servers_in_java)
  end

  def async_set(key, value, expiration = 0, raw = false)
    @client.set(key, expiration, value, transcoder(raw))
  end

  def set(key, value, expiration = 0, raw = false)
    with_timeout async_set(key, value, expiration, raw)
  end

  def async_get(key, raw = false)
    @client.asyncGet(key, transcoder(raw))
  end

  def get(key, raw = false)
    with_timeout async_get(key, raw)
  end

  def incr(key, by = 1)
    with_timeout @client.asyncIncr(key, by)
  end

  def decr(key, by = 1)
    with_timeout @client.asyncDecr(key, by)
  end

  def append(key, value)
    with_timeout @client.append(0, key, value)
  end

  def prepend(key, value)
    with_timeout @client.prepend(0, key, value)
  end

  def multiget(keys, raw = false)
    Hash[*with_timeout(@client.asyncGetBulk(keys, transcoder(raw))).to_a.flatten]
  end
  alias get_multi multiget

  def add(key, value, expiration = 0, raw = false)
    with_timeout @client.add(key, expiration, value, transcoder(raw))
  end

  def del(key)
    with_timeout @client.delete(key)
  end
  alias delete del

  def flush
    @client.flush
  end

  # Return a Hash of statistics responses from the set of servers. Each value is an array with one entry for each server, in the same order the servers were defined.
  def stats(subcommand = nil)
    stats = Hash.new([])
    raw_stats = @client.getStats
    keys = raw_stats.get(raw_stats.keySet.first).keySet

    @servers_in_java.each do |host|
      host_stats = raw_stats[host]
      keys.each do |key|
        value = host_stats[key]
        value = case value
           when /^\d+\.\d+$/ then value.to_f
           when /^\d+$/ then value.to_i
           else value
         end

         stats[key.to_sym] += [value]
      end
    end

    stats
  rescue OperationException => _ # was: rescue Spymemcached::SomeErrorsWereReported => _
    e = _.class.new(e.getType, "Error getting stats")
    e.set_backtrace(_.backtrace)
    raise e
  end

  # Shutdown the connection pool. (Not the remote server, natch.)
  def shutdown
    @client.shutdown
  end

  private
  def transcoder(raw = false)
    raw ? @client.transcoder : @transcoder
  end

  def with_timeout(future, timeout = 0, unit = TimeUnit::MILLISECONDS)
    if timeout > 0
      future.get(timeout, unit)
    else
      future.get
    end
  end
end
