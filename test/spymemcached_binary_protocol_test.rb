require 'test_helper'

class SpymemcachedBinaryProtocolTest < Test::Unit::TestCase
  def setup
    @client = binary_client
    @client.flush_all
  end

  def test_get_set
    assert_equal nil, @client.get('key')
    @client.set('key', 'value')
    assert_equal 'value', @client.get('key')
    assert_equal 'value', @client['key']

    @client.set('key', nil)
    assert_equal nil, @client.get('key')
    @client['key'] = 'hello'
    assert_equal 'hello', @client.get('key')

    @client.set('key2', ['hello', 'world'])
    assert_equal ['hello', 'world'], @client.get('key2')

    @client.set('key2', Msg.new('hi'))
    assert_equal Msg.new('hi'), @client.get('key2')
  end

  def test_get_set_large_string
    str = 'value' * 10_000_000
    @client.set(name, str)
    assert_equal str, @client.get(name)
  end

  def test_add
    assert @client.add('key', 'value')
    assert_equal "value", @client.get('key')

    assert !@client.add('key', 'another')
    assert_equal "value", @client.get('key')
  end

  def test_incr_and_decr
    @client.add('key', '0')
    assert_equal 0, @client.incr('key', 0)
    @client.incr('key')
    assert_equal 1, @client.incr('key', 0)
    @client.incr('key', 5)
    assert_equal 6, @client.incr('key', 0)
    assert_equal '6', @client.get('key')

    @client.set('key', '6')
    @client.decr('key')
    assert_equal 5, @client.decr('key', 0)
    @client.decr('key', 3)
    assert_equal 2, @client.decr('key', 0)

    @client.replace('key', '10')
    assert_equal 11, @client.incr('key')

    @client.cas('key') { '7' }
    assert_equal 8, @client.incr('key')
  end

  def test_fetch
    ret = @client.fetch('key') { 'value' }
    assert_equal 'value', ret
    ret = @client.fetch('key') { 'hello' }
    assert_equal 'value', ret
  end

  def test_get_multi
    @client.add('k1', 'v1')
    @client.add('k2', Msg.new('v2'))
    ret = @client.get_multi('k1', 'k2', 'k3')
    assert_equal(2, ret.size)
    assert_equal('v1', ret['k1'])
    assert_equal(Msg.new('v2'), ret['k2'])
    assert_equal(ret, @client.get_multi(['k1', 'k2', 'k3']))
  end

  def test_cas
    assert_nil(@client.cas('k1') { 'v0' })

    assert @client.add('k1', 'v1')
    assert(@client.cas('k1') { 'v2' })
    assert_equal 'v2', @client.get('k1')

    ret = @client.cas('k1') do
      @client.set('k1', 'v4')
      'v3'
    end
    assert_equal(false, ret)
    assert_equal('v4', @client.get('k1'))
  end

  def test_replace
    assert_equal false, @client.replace('k1', 'v1')
    assert_nil @client['k1']

    @client['k1'] = 'v0'
    assert @client.replace('k1', 'v1')
    assert_equal 'v1', @client['k1']
  end

  def test_delete
    @client['k1'] = 'v0'
    assert @client.delete('k1')
    assert_nil @client['k1']
    assert_equal false, @client.delete('k1')
  end

  def test_append_string
    @client['k1'] = 'v'
    assert @client.append('k1', '1')
    assert_equal 'v1', @client['k1']
  end

  def test_prepend_string
    @client['k1'] = 'v'
    assert @client.prepend('k1', '1')
    assert_equal '1v', @client['k1']
  end
end
