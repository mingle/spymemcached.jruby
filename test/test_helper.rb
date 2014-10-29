require 'test/unit'
$LOAD_PATH << File.join(File.dirname(__FILE__), '..', 'lib')
require "spymemcached"

class Msg < Struct.new(:name)
end

class Test::Unit::TestCase
  def plain_client
    @@plain_client ||= spymemcached(:binary => false)
  end

  def binary_client
    @@binary_client ||= spymemcached
  end

  def spymemcached(opts={})
    Spymemcached.new('localhost:11211', opts)
  end
end
