package org.apache.ignite.internal.processors.cache.distributed.constraint;

import java.math.BigDecimal;
import org.apache.ignite.Ignite;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

public class BugTest extends GridCommonAbstractTest {

    static class A {
        public void test(Object o) {
            System.out.println("o");
        }

        public void test(String o) {
            System.out.println("s");
        }

        public <T> T getT(Object o) {
            return (T) o;
        }
    }

    public static void main(String[] args) {
        final A a = new A();
        a.test(a.getT(BigDecimal.ZERO));
    }

    @Test
    public void testBinaryObject() throws Exception {
        try (Ignite ignite = startGrid(0)) {
            BinaryObjectBuilder builder = ignite.binary()
                .builder("testVal")
                .setField("name", "John Doe", String.class);

            builder.setField("name", builder.getField("name"));
        }
    }
}
