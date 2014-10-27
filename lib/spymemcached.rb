require 'spymemcached-2.11.4.jar'
require 'spymemcached_adapter.jar'
require 'spymemcached_adapter'

#
# Memcached client Spymemcached JRuby extension
#
class Spymemcached
  class Error < StandardError; end
  class TimeoutError < Error; end

  # default options for client
  DEFAULT_OPTIONS = {
    :timeout => 0.5, # second
    :binary => true
  }

  # Accepts a list of +servers+ and a list of +options+.
  # The +servers+ can be either strings of the form "hostname:port" or
  # one string format as "<hostname>:<port>,<hostname>:<port>".
  # Use KetamaConnectionFactory as Spymemcached MemcachedClient connection factory.
  # See Spymemcached document for details.
  # Different with Ruby memcache clients (e.g. Dalli), there is no raw option for operations.
  #
  # Valid +options+ are:
  #
  #   [:namespace]   Prepends this value to all keys added or retrieved.
  #   [:timeout]     Time to use as the socket read timeout, seconds.  Defaults to 0.5 sec.
  #   [:binary]      Talks binary protocol with Memcached server. Default to true.
  #
  # Logger: see Spymemcached for how to turn on detail log
  #
  def initialize(servers=['localhost:11211'], options={})
    @servers, @options = Array(servers).join(','), DEFAULT_OPTIONS.merge(options)
    @client = SpymemcachedAdapter.new(@servers, @options)
    @namespace = if @options[:namespace]
      @options[:namespace].is_a?(Proc) ? @options[:namespace] : lambda { @options[:namespace] }
    end
    at_exit { shutdown }
  end

  def fetch(key, ttl=0, &block)
    val = get(key)
    if val.nil? && block_given?
      val = yield
      add(key, val, ttl)
    end
    val
  end

  def get(key)
    @client.get(ns(key))
  end
  alias :[] :get

  def get_multi(*keys)
    Hash[@client.get_multi(keys.map(&method(:ns))).map {|k, v| [unns(k), v]}]
  end

  def add(key, value, ttl=0, opts={})
    @client.add(ns(key), value, ttl)
  end

  def set(key, value, ttl=0, opts={})
    @client.set(ns(key), value, ttl)
  end
  alias :[]= :set

  def cas(key, ttl=0, &block)
    @client.cas(ns(key), ttl, &block)
  end

  def replace(key, value, ttl=0)
    @client.replace(ns(key), value, ttl)
  end

  def delete(key)
    @client.delete(ns(key))
  end

  def incr(key, by=1)
    @client.incr(ns(key), by)
  end

  def decr(key, by=1)
    @client.decr(ns(key), by)
  end

  def append(key, value)
    @client.append(ns(key), value)
  end

  def prepend(key, value)
    @client.prepend(ns(key), value)
  end

  def touch(key, ttl=0)
    @client.touch(ns(key), ttl)
  end

  def stats
    @client.stats
  end

  def version
    @client.version
  end

  def flush_all
    @client.flush_all
  end
  alias :flush :flush_all
  alias :clear :flush_all

  def shutdown
    @client.shutdown
  end

  # compatible api
  def rails23
    require 'spymemcached/rails23'
    Rails23.new(self)
  end

  private
  def raw?(opts)
    opts.is_a?(Hash) ? opts[:raw] : opts
  end

  def ns(key)
    return key unless namespace
     "#{namespace.call}:#{key}"
  end

  def unns(k)
    return k unless namespace
    @ns_size ||= namespace.call.size + 1
    k[@ns_size..-1]
  end

  def namespace
    @namespace
  end
end
