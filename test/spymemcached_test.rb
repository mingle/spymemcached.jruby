# encoding: UTF-8
require 'test_helper'

class SpymemcachedTest < Test::Unit::TestCase
  def setup
    @client = binary_client
    @client.flush_all
  end

  def test_expiry
    @client.add('add_key1', 'v1', 0)
    @client.add('add_key2', 'v2', 2)

    @client.set('set_key1', 'v1', 0)
    @client.set('set_key2', 'v2', 2)

    @client.fetch('fetch_key1', 0) { 'v1' }
    @client.fetch('fetch_key2', 2) { 'v2' }

    @client.add('cas_key1', 'v0')
    @client.add('cas_key2', 'v0')
    @client.cas('cas_key1', 0) { 'v1' }
    @client.cas('cas_key2', 2) { 'v2' }

    @client.add('replace_key1', 'v0')
    @client.add('replace_key2', 'v0')
    @client.replace('replace_key1', 'v1', 0)
    @client.replace('replace_key2', 'v2', 2)

    @client.add('touch_key1', 'v1', 2)

    sleep 0.1

    assert_equal 'v2', @client.get('add_key2')
    assert_equal 'v2', @client.get('set_key2')
    assert_equal 'v2', @client.get('fetch_key2')
    assert_equal 'v2', @client.get('cas_key2')
    assert_equal 'v2', @client.get('replace_key2')

    assert @client.touch('touch_key1')

    sleep 2
    assert_equal 'v1', @client['touch_key1']

    assert_equal 'v1', @client.get('add_key1')
    assert_nil @client.get('add_key2')

    assert_equal 'v1', @client.get('set_key1')
    assert_nil @client.get('set_key2')

    assert_equal 'v1', @client.get('fetch_key1')
    assert_nil @client.get('fetch_key2')

    assert_equal 'v1', @client.get('cas_key1')
    assert_nil @client.get('cas_key2')

    assert_equal 'v1', @client.get('replace_key1')
    assert_nil @client.get('replace_key2')
  end

  def test_timeout
    @client = Spymemcached.new('localhost:11111', :timeout => 0.01)
    assert_timeout = lambda do |action, *args|
      start_at = Time.now
      assert_raise Spymemcached::TimeoutError do
        @client.send(action, *args) { 'v1' }
      end
      time = Time.now - start_at
      assert time < 0.025, "Timeout is 0.01, actual time of action #{action} when timeout: #{time}"
    end

    assert_timeout[:flush_all]

    assert_timeout[:get, 'key']
    assert_timeout[:get_multi, 'key']
    assert_timeout[:incr, 'key']
    assert_timeout[:decr, 'key']
    assert_timeout[:fetch, 'key']
    assert_timeout[:delete, 'key']
    assert_timeout[:touch, 'key']

    assert_timeout[:add, 'key', 'value']
    assert_timeout[:set, 'key', 'value']
    assert_timeout[:replace, 'key', 'value']
    assert_timeout[:append, 'key', 'value']
    assert_timeout[:prepend, 'key', 'value']

    assert_equal({}, @client.version)

    stats = @client.stats
    assert_equal(1, stats.size)
    assert_equal([{}], stats.values)
  end

  def test_namespace
    @ns_client = Spymemcached.new('localhost:11211', :namespace => 'ns')
    assert_ns = lambda do |k, v|
      assert_equal 'v1', @ns_client.get('key1')
      assert_nil @client.get('key1')
      assert_equal 'v1', @client.get('ns:key1')
    end

    @ns_client.set('key1', 'v1')
    assert_ns.call('key1', 'v1')

    @ns_client.add('key2', 'v2')
    assert_ns.call('key2', 'v2')

    assert_equal({'key1' => 'v1', 'key2' => 'v2'}, @ns_client.get_multi('key1', 'key2'))
    assert_equal({}, @client.get_multi('key1', 'key2'))

    @ns_client.add('key3', 'v0')
    @ns_client.cas('key3') { 'v3' }
    assert_equal 'v3', @ns_client['key3']
    assert_nil @client['key3']
    assert_equal 'v3', @client['ns:key3']

    assert @ns_client.replace('key3', 'v4')
    assert_equal 'v4', @ns_client['key3']
    assert_nil @client['key3']
    assert_equal 'v4', @client['ns:key3']

    @client['key3'] = 'v3'
    assert @ns_client.delete('key3')
    assert_equal 'v3', @client['key3']
    assert_nil @ns_client['key3']

    @ns_client.append('key2', '4')
    assert_equal 'v24', @ns_client.get('key2')

    @ns_client.prepend('key2', '4')
    assert_equal '4v24', @ns_client.get('key2')

    @ns_client['key2'] = '1'
    assert_equal 2, @ns_client.incr('key2')
    assert_equal 1, @ns_client.decr('key2')

    @ns_client.touch('key2', 1)
    sleep 1.1
    assert_nil @ns_client['key2']
  end

  def test_lambda_as_namespace
    @ns_client = Spymemcached.new('localhost:11211', :namespace => lambda { 'ns' })
    @ns_client['key'] = 'value'
    assert_equal 'value', @client['ns:key']

    assert_equal({'key' => "value"}, @ns_client.get_multi('key'))
  end

  def test_timeout_zero
    assert_raise Spymemcached::Error do
      Spymemcached.new('localhost:11111', :timeout => 0)
    end
  end

  def test_stats
    stats = @client.stats
    assert_equal(Hash, stats.class)
    assert_equal(1, stats.size)
    assert_match(/localhost/, stats.keys.first)
    assert_match(/\:11211/, stats.keys.first)
    assert_equal(Hash, stats.values.first.class)
    assert stats.values.first.size > 5
    assert_match(@client.version.values.first, stats.values.first['version'])
  end

  def test_version
    v = @client.version
    assert_equal(Hash, v.class)
    assert_equal(1, v.size)
    assert_match(/localhost/, v.keys.first)
    assert_match(/\:11211/, v.keys.first)
    assert_match(/\d\.\d+\.\d+/, v.values.first)
  end

  def test_handles_invalid_key_and_utf8_chars
    k = 'k 开'
    @client.add(k, 'v1')
    assert_equal('v1', @client.get(k))
    @client.set(k, 'v2')
    assert_equal({k => 'v2'}, @client.get_multi(k))
  end

  def test_handles_long_string_key
    k = 'k 开' * 250
    @client.add(k, 'v1')
    assert_equal('v1', @client.get(k))
    @client.set(k, 'v2')
    assert_equal({k => 'v2'}, @client.get_multi(k))
  end
end
