# Spymemcached.jruby

A JRuby extension wraps the latest spymemcached client.
Fastest jruby memcached client, threadsafe.

## Usage

Start a local networked memcached server:

    $ memcached -p 11211

Require the library and instantiate a Spymemcached object at a global level:

    require 'spymemcached'
    $cache = Spymemcached.new("localhost:11211")

Setup multiple servers with options

    require 'spymemcached'
    $cache = Spymemcached.new(['memcached1.host:11211', 'memcached2.host:11211', 'memcached3.host:11211'],
                              {:namespace => 'appName', :timeout => 0.1, :binary => true})

Valid +options+ are:

    [:namespace]        Prepends this value to all keys added or retrieved.
    [:timeout]          Time to use as the socket read timeout, seconds.  Defaults to 0.5 sec.
    [:binary]           Talks binary protocol with Memcached server. Default to true.
    [:should_optimize]  If true, Spymemcached low-level optimization is in effect. Default to false.

### Rails 4

Use [spymemcached_store](https://github.com/ThoughtWorksStudios/spymemcached_store) gem to integrate ActiveSupport cache store and spymemcached.jruby gem.

### Rails 3

    require 'spymemcached'
    config.cache_store = :mem_cache_store, Spymemcached.new(servers, options).rails23

### Rails 2.x

    require 'spymemcached'
    ActionController::Base.cache_store = :mem_cache_store, Spymemcached.new(servers, options).rails23

## Compatibility

The most important different between spymemcached.jruby and other non spymemcached backended memcache client is the support of option :raw.

As Spymemcached provides mechanism to encode and decode cache data, spymemcached.jruby does marshal dump & load for Ruby object in Java except Ruby string and integer.

Any Ruby String or Integer type cache data will be directly converted to Java String or Integer.

Hence we don't consider :raw option in spymemcached.jruby.

However, Rails cache store supports :raw option too when doing read and write on local cache.
This behaviour is kept as I'm not sure what to do with it. I'm happy to fix it if you found anything wrong with it.

## Default behaviors

Spymemcached.jruby applies:

* Ketama key hash algorithm (see Spymemcached document or [RJ's blog post](http://www.last.fm/user/RJ/journal/2007/04/10/392555/) for details)
* Gzip compressed when the cache data size is larger than 16kb.
* Binary protocol

Other default settings see Spymemcached DefaultConnectionFactory for details.

## Performance

[Benchmark result](https://github.com/ThoughtWorksStudios/memcached-client-benchmark) compared with gem dalli and jruby-memcached

## Further resources

* [Spymemcached](https://code.google.com/p/spymemcached/)
* [Spymemcached Optimizations](https://code.google.com/p/spymemcached/wiki/Optimizations)
