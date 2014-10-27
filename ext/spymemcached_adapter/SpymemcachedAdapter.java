import net.spy.memcached.*;
import net.spy.memcached.internal.CheckedOperationTimeoutException;
import net.spy.memcached.protocol.binary.BinaryOperationFactory;
import net.spy.memcached.transcoders.SerializingTranscoder;
import org.jruby.*;
import org.jruby.anno.JRubyClass;
import org.jruby.anno.JRubyMethod;
import org.jruby.javasupport.JavaUtil;
import org.jruby.runtime.Block;
import org.jruby.runtime.ThreadContext;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.marshal.MarshalStream;
import org.jruby.runtime.marshal.UnmarshalStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.SocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by Xiao Li <swing1979@gmail.com> on 10/24/14.
 */
@JRubyClass(name = "SpymemcachedAdapter")
public class SpymemcachedAdapter extends RubyObject {

    private Long timeout;

    public static class IRubyObjectTranscoder extends SerializingTranscoder {
        private Ruby ruby;

        public IRubyObjectTranscoder(Ruby ruby) {
            super();
            this.ruby = ruby;
        }

        @Override
        public Object decode(CachedData d) {
            Object ret = super.decode(d);
            if (ret instanceof byte[]) {
                byte[] bytes = (byte[]) ret;
                try {
                    return new UnmarshalStream(ruby, new ByteArrayInputStream(bytes), null, false, false).unmarshalObject();
                } catch (IOException e) {
                    throw ruby.newIOErrorFromException(e);
                }
            } else {
                return JavaUtil.convertJavaToRuby(ruby, ret);
            }
        }

        @Override
        public CachedData encode(Object o) {
            if (o instanceof IRubyObject) {
                ByteArrayOutputStream stream = new ByteArrayOutputStream();
                try {
                    MarshalStream marshal = new MarshalStream(ruby, stream, Integer.MAX_VALUE);
                    marshal.dumpObject((IRubyObject) o);
                } catch (IOException e) {
                    throw ruby.newIOErrorFromException(e);
                }
                return super.encode(stream.toByteArray());
            } else {
                return super.encode(o);
            }
        }
    }

    private MemcachedClient client;
    private Ruby ruby;

    public SpymemcachedAdapter(Ruby ruby, RubyClass klazz) {
        super(ruby, klazz);
        this.ruby = ruby;
    }

    @JRubyMethod(required = 2)
    public IRubyObject initialize(IRubyObject servers, IRubyObject opts) {
        RubyHash hash = opts.convertToHash();
        timeout = getTimeout(hash);
        client = initClient(servers.asJavaString(), hash);
        return ruby.getNil();
    }

    @JRubyMethod(required = 1)
    public IRubyObject get(IRubyObject key) {
        return obj(client.asyncGet(key.asJavaString()));
    }

    @JRubyMethod(required = 1)
    public IRubyObject get_multi(IRubyObject keys) {
        List<String> list = Arrays.asList((String[]) keys.convertToArray().toArray(new String[0]));
        return obj(client.asyncGetBulk(list));
    }

    @JRubyMethod(required = 3)
    public IRubyObject add(IRubyObject key, IRubyObject value, IRubyObject ttl) {
        return obj(client.add(key.asJavaString(), integer(ttl), preprocess(value)));
    }

    @JRubyMethod(required = 3)
    public IRubyObject set(IRubyObject key, IRubyObject value, IRubyObject ttl) {
        return obj(client.set(key.asJavaString(), integer(ttl), preprocess(value)));
    }

    @JRubyMethod(required = 3)
    public IRubyObject replace(IRubyObject key, IRubyObject value, IRubyObject ttl) {
        return obj(client.replace(key.asJavaString(), integer(ttl), preprocess(value)));
    }

    @JRubyMethod(required = 1)
    public IRubyObject delete(IRubyObject key) {
        return obj(client.delete(key.asJavaString()));
    }

    @JRubyMethod(required = 2)
    public IRubyObject append(IRubyObject key, IRubyObject value) {
        return obj(client.append(key.asJavaString(), preprocess(value)));
    }

    @JRubyMethod(required = 2)
    public IRubyObject prepend(IRubyObject key, IRubyObject value) {
        return obj(client.prepend(key.asJavaString(), preprocess(value)));
    }

    @JRubyMethod(required = 2)
    public IRubyObject cas(ThreadContext tc, IRubyObject key, IRubyObject ttl, Block block) {
        String k = key.asJavaString();
        CASValue<Object> casValue = client.gets(k);
        if (casValue == null) {
            return ruby.getNil();
        }
        long casId = casValue.getCas();
        IRubyObject value = block.call(tc);
        CASResponse response = (CASResponse) futureGet(client.asyncCAS(k, casId, integer(ttl), preprocess(value)));
        if (response == CASResponse.OK) {
            return ruby.getTrue();
        } else if (response == CASResponse.NOT_FOUND) {
            return ruby.getNil();
        } else {
            return ruby.getFalse();
        }
    }

    @JRubyMethod(required = 2)
    public IRubyObject incr(IRubyObject key, IRubyObject by) {
        return obj(client.asyncIncr(key.asJavaString(), integer(by)));
    }

    @JRubyMethod(required = 2)
    public IRubyObject decr(IRubyObject key, IRubyObject by) {
        return obj(client.asyncDecr(key.asJavaString(), integer(by)));
    }

    @JRubyMethod(required = 2)
    public IRubyObject touch(IRubyObject key, IRubyObject ttl) {
        return obj(client.touch(key.asJavaString(), integer(ttl)));
    }

    @JRubyMethod
    public IRubyObject version(ThreadContext context) {
        RubyHash results = RubyHash.newHash(ruby);
        Map<SocketAddress, String> versions = client.getVersions();
        for(Map.Entry<SocketAddress, String> entry : versions.entrySet()) {
            results.op_aset(context, ruby.newString(entry.getKey().toString()), ruby.newString(entry.getValue()));
        }
        return results;
    }

    @JRubyMethod
    public IRubyObject stats(ThreadContext context) {
        RubyHash results = RubyHash.newHash(ruby);
        for(Map.Entry<SocketAddress, Map<String, String>> entry : client.getStats().entrySet()) {
            RubyHash serverHash = RubyHash.newHash(ruby);
            for(Map.Entry<String, String> server : entry.getValue().entrySet()) {
                serverHash.op_aset(context, ruby.newString(server.getKey()), ruby.newString(server.getValue()));
            }
            results.op_aset(context, ruby.newString(entry.getKey().toString()), serverHash);
        }
        return results;
    }

    @JRubyMethod
    public IRubyObject flush_all() {
        return obj(client.flush());
    }

    @JRubyMethod
    public IRubyObject shutdown() {
        client.shutdown();
        return ruby.getNil();
    }

    private MemcachedClient initClient(String servers, Map opts) {
        ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder(new KetamaConnectionFactory());
        if (isBinary(opts)) {
            builder.setOpFact(new BinaryOperationFactory());
        }
        builder.setTranscoder(new IRubyObjectTranscoder(ruby));
        builder.setOpTimeout(timeout);
        try {
            return new MemcachedClient(builder.build(), AddrUtil.getAddresses(servers));
        } catch (IllegalArgumentException e) {
            throw ruby.newRaiseException(getError(), e.getLocalizedMessage());
        } catch (IOException e) {
            throw ruby.newIOErrorFromException(e);
        }
    }

    private RubyClass getTimeoutError() {
        return ruby.getClass("Spymemcached").getClass("TimeoutError");
    }

    private RubyClass getError() {
        return ruby.getClass("Spymemcached").getClass("Error");
    }

    private Long getTimeout(Map opts) {
        RubySymbol sym = ruby.newSymbol("timeout");
        return (long) (((Number) opts.get(sym)).doubleValue() * 1000);
    }

    private boolean isBinary(Map opts) {
        RubySymbol sym = ruby.newSymbol("binary");
        return (Boolean) opts.get(sym);
    }

    private int integer(IRubyObject obj) {
        return (int) obj.convertToInteger().getLongValue();
    }

    private IRubyObject obj(Future future) {
        Object obj = futureGet(future);
        if (obj instanceof IRubyObject) {
            return (IRubyObject) obj;
        } else {
            return JavaUtil.convertJavaToRuby(ruby, obj);
        }
    }

    private Object futureGet(Future future) {
        try {
            return future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw ruby.newRaiseException(getError(), "InterruptedException waiting for value: " + e.getLocalizedMessage());
        } catch (ExecutionException e) {
            throw ruby.newRaiseException(getError(), "ExecutionException waiting for value: " + e.getLocalizedMessage());
        } catch (TimeoutException e) {
            throw ruby.newRaiseException(getTimeoutError(), e.getLocalizedMessage());
        } catch (RuntimeException e) {
            if (e.getCause() instanceof CheckedOperationTimeoutException) {
                throw ruby.newRaiseException(getTimeoutError(), e.getLocalizedMessage());
            }
            throw e;
        }
    }

    private Object preprocess(IRubyObject value) {
        if (value instanceof RubyString) {
            return value.asJavaString();
        } else if (value instanceof RubyInteger) {
            return ((RubyInteger) value).getLongValue();
        } else {
            return value;
        }
    }

}
