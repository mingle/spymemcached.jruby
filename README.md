Spymemcached.jruby
================

A JRuby extension wraps the latest spymemcached client.

Usage
----------------

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

    [:namespace]   Prepends this value to all keys added or retrieved.
    [:timeout]     Time to use as the socket read timeout, seconds.  Defaults to 0.5 sec.
    [:binary]      Talks binary protocol with Memcached server. Default to true.

Rails 4
--------------------

Use [spymemcached_store](https://github.com/ThoughtWorksStudios/spymemcached_store) gem to integrate ActiveSupport cache store and spymemcached.jruby gem.

Rails 2.3
--------------------

    ActionController::Base.cache_store = :mem_cache_store, Spymemcached.new(servers).rails23

Performance
---------------

[Benchmark result](https://github.com/ThoughtWorksStudios/memcached-client-benchmark) compared with gem dalli and jruby-memcached
