require 'memcache'

class Spymemcached
  class Rails23
    module Response # :nodoc:
      STORED      = "STORED\r\n"
      NOT_STORED  = "NOT_STORED\r\n"
      EXISTS      = "EXISTS\r\n"
      NOT_FOUND   = "NOT_FOUND\r\n"
      DELETED     = "DELETED\r\n"
    end

    def initialize(client)
      @client = client
    end

    def get_multi(*args)
      if [Hash, TrueClass, FalseClass].include?(args.last)
        args.pop
      end
      op(:get_multi, *args)
    end

    def get(key, *args)
      op(:get, key)
    end

    def add(key, value, ttl=0, raw=false)
      op(:add, key, value, ttl, opts(raw)) ? Response::STORED : Response::NOT_STORED
    end

    def set(key, value, ttl=0, raw=false)
      op(:set, key, value, ttl, opts(raw)) ? Response::STORED : Response::NOT_STORED
    end

    def delete(key, *args)
      op(:delete, key) ? Response::DELETED : Response::NOT_FOUND
    end

    def incr(key, by=1, ttl=0)
      op(:incr, key, by, ttl)
    end

    def decr(key, by=1, ttl=0)
      op(:decr, key, by, ttl)
    end

    def flush_all
      op(:flush_all)
    end

    def stats
      op(:stats)
    end

    def shutdown
      op(:shutdown)
    end

    private
    def opts(raw)
      raw.is_a?(Hash) ? raw : {:raw => raw}
    end

    def op(name, *args, &block)
      @client.send(name, *args, &block)
    rescue Spymemcached::Error => e
      raise MemCache::MemCacheError, e.message
    end
  end
end
