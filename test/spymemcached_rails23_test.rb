require 'test_helper'

class SpymemcachedRails23Test < Test::Unit::TestCase
  def setup
    @client = binary_client.rails23
    @client.flush_all
  end

  def test_api_compatible
    assert_equal response::STORED, @client.add("key", 'value')
    assert_equal 'value', @client.get('key')
    assert_equal response::STORED, @client.set('key', 'value2')

    assert_equal response::DELETED, @client.delete('key')
    assert_equal response::NOT_FOUND, @client.delete('key2')

    @client.set("key1", '0', 0, :raw => true)
    @client.set("key2", '1', 0, :raw => true)
    assert_equal({"key1" => '0', 'key2' => '1'}, @client.get_multi('key1', 'key2'))

    assert_equal 1, @client.incr('key1', 1, ttl=0)
    assert_equal 0, @client.decr('key2', 1, ttl=0)

    assert @client.stats.values.first
  end

  def test_rails3_api_compatible
    @client.set("key1", '0', 0, :raw => true)
    @client.set("key2", '1', 0, :raw => true)
    assert_equal({"key1" => '0', 'key2' => '1'}, @client.get_multi(['key1', 'key2'], :raw => true))
  end

  def response
    Spymemcached::Rails23::Response
  end
end
