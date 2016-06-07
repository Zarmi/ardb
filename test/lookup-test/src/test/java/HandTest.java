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
        jedis.set("lolololofdlfd", "string");
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

    @After
    public void afterTest() {
        flushAll();
    }

    @AfterClass
    public static void after() {
        jedis.close();
    }
}
