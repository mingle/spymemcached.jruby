import net.spy.memcached.*;
import net.spy.memcached.internal.CheckedOperationTimeoutException;
import net.spy.memcached.internal.OperationFuture;
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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by Xiao Li <swing1979@gmail.com> on 10/24/14.
 */
@JRubyClass(name = "SpymemcachedAdapter")
public class SpymemcachedAdapter extends RubyObject {

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
        client = initClient(servers.asJavaString(), opts.convertToHash());
        return ruby.getNil();
    }

    @JRubyMethod(required = 1)
    public IRubyObject get(IRubyObject key) {
        try {
            return (IRubyObject) client.get(key.asJavaString());
        } catch (OperationTimeoutException e) {
            throw ruby.newRaiseException(getTimeoutError(), e.getLocalizedMessage());
        }
    }

    @JRubyMethod(required = 1)
    public IRubyObject get_multi(IRubyObject keys) {
        try {
            return obj(client.getBulk(Arrays.asList((String[]) keys.convertToArray().toArray(new String[0]))));
        } catch (OperationTimeoutException e) {
            throw ruby.newRaiseException(getTimeoutError(), e.getLocalizedMessage());
        }
    }

    @JRubyMethod(required = 3)
    public IRubyObject add(IRubyObject key, IRubyObject value, IRubyObject ttl) {
        try {
            return bool(client.add(key.asJavaString(), integer(ttl), preprocess(value)));
        } catch (RuntimeException e) {
            if (e.getCause() instanceof CheckedOperationTimeoutException) {
                throw ruby.newRaiseException(getTimeoutError(), e.getLocalizedMessage());
            }
            throw e;
        }
    }

    @JRubyMethod(required = 3)
    public IRubyObject set(IRubyObject key, IRubyObject value, IRubyObject ttl) {
        try {
            return bool(client.set(key.asJavaString(), integer(ttl), preprocess(value)));
        } catch (RuntimeException e) {
            if (e.getCause() instanceof CheckedOperationTimeoutException) {
                throw ruby.newRaiseException(getTimeoutError(), e.getLocalizedMessage());
            }
            throw e;
        }
    }

    @JRubyMethod(required = 3)
    public IRubyObject replace(IRubyObject key, IRubyObject value, IRubyObject ttl) {
        try {
            return bool(client.replace(key.asJavaString(), integer(ttl), preprocess(value)));
        } catch (RuntimeException e) {
            if (e.getCause() instanceof CheckedOperationTimeoutException) {
                throw ruby.newRaiseException(getTimeoutError(), e.getLocalizedMessage());
            }
            throw e;
        }
    }

    @JRubyMethod(required = 1)
    public IRubyObject delete(IRubyObject key) {
        try {
            return bool(client.delete(key.asJavaString()));
        } catch (RuntimeException e) {
            if (e.getCause() instanceof CheckedOperationTimeoutException) {
                throw ruby.newRaiseException(getTimeoutError(), e.getLocalizedMessage());
            }
            throw e;
        }
    }

    @JRubyMethod(required = 2)
    public IRubyObject append(IRubyObject key, IRubyObject value) {
        try {
            return bool(client.append(key.asJavaString(), preprocess(value)));
        } catch (RuntimeException e) {
            if (e.getCause() instanceof CheckedOperationTimeoutException) {
                throw ruby.newRaiseException(getTimeoutError(), e.getLocalizedMessage());
            }
            throw e;
        }
    }

    @JRubyMethod(required = 2)
    public IRubyObject prepend(IRubyObject key, IRubyObject value) {
        try {
            return bool(client.prepend(key.asJavaString(), preprocess(value)));
        } catch (RuntimeException e) {
            if (e.getCause() instanceof CheckedOperationTimeoutException) {
                throw ruby.newRaiseException(getTimeoutError(), e.getLocalizedMessage());
            }
            throw e;
        }
    }

    @JRubyMethod(required = 2)
    public IRubyObject cas(ThreadContext tc, IRubyObject key, IRubyObject ttl, Block block) {
        String k = key.asJavaString();
        try {
            CASValue<Object> casValue = client.gets(k);
            if (casValue == null) {
                return ruby.getNil();
            }
            long casId = casValue.getCas();
            IRubyObject value = block.call(tc);
            CASResponse response = client.cas(k, casId, integer(ttl), preprocess(value));
            if (response == CASResponse.OK) {
                return ruby.getTrue();
            } else if (response == CASResponse.NOT_FOUND) {
                return ruby.getNil();
            } else {
                return ruby.getFalse();
            }
        } catch (OperationTimeoutException e) {
            throw ruby.newRaiseException(getTimeoutError(), e.getLocalizedMessage());
        }
    }

    @JRubyMethod(required = 2)
    public IRubyObject incr(IRubyObject key, IRubyObject by) {
        try {
            return obj(client.incr(key.asJavaString(), integer(by)));
        } catch (OperationTimeoutException e) {
            throw ruby.newRaiseException(getTimeoutError(), e.getLocalizedMessage());
        }
    }

    @JRubyMethod(required = 2)
    public IRubyObject decr(IRubyObject key, IRubyObject by) {
        try {
            return obj(client.decr(key.asJavaString(), integer(by)));
        } catch (OperationTimeoutException e) {
            throw ruby.newRaiseException(getTimeoutError(), e.getLocalizedMessage());
        }
    }

    @JRubyMethod(required = 2)
    public IRubyObject touch(IRubyObject key, IRubyObject ttl) {
        try {
            return bool(client.touch(key.asJavaString(), integer(ttl)));
        } catch (RuntimeException e) {
            if (e.getCause() instanceof CheckedOperationTimeoutException) {
                throw ruby.newRaiseException(getTimeoutError(), e.getLocalizedMessage());
            }
            throw e;
        }
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
        try {
            return bool(client.flush());
        } catch (RuntimeException e) {
            if (e.getCause() instanceof CheckedOperationTimeoutException) {
                throw ruby.newRaiseException(getTimeoutError(), e.getLocalizedMessage());
            }
            throw e;
        }
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
        builder.setOpTimeout(getTimeout(opts));
        try {
            return new MemcachedClient(builder.build(), AddrUtil.getAddresses(servers));
        } catch (IOException e) {
            throw ruby.newIOErrorFromException(e);
        }
    }

    private RubyClass getTimeoutError() {
        return ruby.getClass("Spymemcached").getClass("TimeoutError");
    }

    private Long getTimeout(Map opts) {
        RubySymbol sym = ruby.newSymbol("timeout");
        return (long) (((Double) opts.get(sym)) * 1000);
    }

    private boolean isBinary(Map opts) {
        RubySymbol sym = ruby.newSymbol("binary");
        return (Boolean) opts.get(sym);
    }

    private int integer(IRubyObject obj) {
        return (int) obj.convertToInteger().getLongValue();
    }

    private IRubyObject bool(OperationFuture<Boolean> future) {
        try {
            return obj(future.get().booleanValue());
        } catch (InterruptedException e) {
            throw ruby.newThreadError(e.getLocalizedMessage());
        } catch (ExecutionException e) {
            throw ruby.newRuntimeError(e.getLocalizedMessage());
        }
    }

    private IRubyObject obj(Object obj) {
        return JavaUtil.convertJavaToRuby(ruby, obj);
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
