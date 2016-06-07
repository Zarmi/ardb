import org.junit.*;
import redis.clients.jedis.Jedis;

import java.util.Set;

import static utils.TestUtils.*;

/**
 * Created by pva701 on 06.06.16.
 */

public class HandTest {

    static Jedis jedis;

    @BeforeClass
    public static void init() {
        jedis = createJedis();
        flushAll();
    }

    @Test
    public void testPatterns() {
        String[] keysArray = new String[] {
                "keyabracadabra",
                "keykeyfdfd",
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaabracadaaaaaaa",
                "randomstring",
                "lolololofdlfd",
                "lol333",
                "afdflolfdjfdhfllool",
                "fdkfdjklol",
                "fdkfdjkstringstrin",
                "lolfdjfhdjfhjdf"};

        Set<String> keys = toSortedSet(keysArray);

        for (String key: keysArray)
            jedis.hmset(key, genRandomMap(fields));
        Assert.assertEquals(toSortedSet(keysArray), jedis.keys("*"));
        testPattern(keys, jedis, "lo*");

        remove(keys, jedis, "lol333");
        testPattern(keys, jedis, "lo*");

        testPattern(keys, jedis, "??????aaaaa");
        testPattern(keys, jedis, "*fd*");
        testPattern(keys, jedis, "*br?c");
        testPattern(keys, jedis, "fd*st*in");
        testPattern(keys, jedis, "*fd*st*i*");
        testPattern(keys, jedis, "*fd*st*i?");
        testPattern(keys, jedis, "lolololofdlfd");
        testPattern(keys, jedis, "*Strin*");
        testPattern(keys, jedis, "*strin*");
        testPattern(keys, jedis, "??y*");
        jedis.hmset("lolololofdlfd", genRandomMap(fields));
        testPattern(keys, jedis, "*lol*");
        remove(keys, jedis, "lolololofdlfd");

        testPattern(keys, jedis, "*lol*");
        testPattern(keys, jedis, "key[ak]");
        testPattern(keys, jedis, "lol[fo3]");
        add(keys, jedis, "lol))))");
        add(keys, jedis, "lol((((");
        add(keys, jedis, "lol]]]");
        add(keys, jedis, "lol[[[");
        testPattern(keys, jedis, "lol[fo3\\)\\(]");
        testPattern(keys, jedis, "lol[fo3\\)\\(\\]\\[]");
    }

    @Test
    public void ttlTest() throws Exception {
        try (Jedis jedis = createJedis()) {
            final String[] fields = new String[]{"a", "b", "c"};
            jedis.hmset("key1", genRandomMap(fields));
            jedis.expire("key1", 6);
            jedis.hmset("key2", genRandomMap(fields));
            jedis.expire("key2", 3);
            Thread.sleep(4000);
            Set<String> set = jedis.keys("*");
            Assert.assertFalse(set.contains("key2"));
            Assert.assertTrue(set.contains("key1"));
            Thread.sleep(3000);
            Assert.assertFalse(jedis.keys("*").contains("key1"));
            Assert.assertFalse(jedis.keys("*").contains("key2"));

            final int numKeys = 10;
            for (int i = 1; i <= numKeys; ++i) {
                String key = "key" + i;
                jedis.hmset(key, genRandomMap(fields));
                jedis.expire(key, i);
            }

            Thread.sleep(500);
            for (int i = 1; i <= numKeys; ++i) {
                Thread.sleep(1000);
                Assert.assertFalse(jedis.keys("*").contains("key" + i));
                Assert.assertFalse(jedis.keys("key" + i + "*").contains("key" + i));
            }
        }
    }


    @After
    public void afterTest() {
        flushAll();
    }

    @AfterClass
    public static void after() {
        jedis.close();
    }
}
