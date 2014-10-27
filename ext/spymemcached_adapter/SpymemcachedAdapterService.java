import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.load.BasicLibraryService;

import java.io.IOException;

/**
 * Created by Xiao Li <swing1979@gmail.com> on 10/24/14.
 */
public class SpymemcachedAdapterService implements BasicLibraryService {
    public boolean basicLoad(final Ruby ruby) throws IOException {
        RubyClass yc = ruby.defineClass("SpymemcachedAdapter", ruby.getObject(), new ObjectAllocator() {
            public IRubyObject allocate(Ruby ruby, RubyClass klazz) {
                return new SpymemcachedAdapter(ruby, klazz);
            }
        });
        yc.defineAnnotatedMethods(SpymemcachedAdapter.class);
        return true;
    }
}