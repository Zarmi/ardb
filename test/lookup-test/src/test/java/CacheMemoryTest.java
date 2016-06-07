import org.junit.Before;
import org.junit.Test;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.Map;

import static utils.TestUtils.*;

/**
 * Created by pva701 on 07.06.16.
 */
public class CacheMemoryTest {

    @Before
    public void beforeTest() {
        flushAll();
    }

    @Test
    public void memoryTest() {
        final int ITERATIONS = 1_000_000;
        try (Jedis jedis = createJedis()) {
            for (int i = 1; i <= ITERATIONS; ++i) {
                int prefLen = randInt(5, 15);
                String key = randomString(prefLen) + "_" + randomString(32);
                Map<String, String> mp = new HashMap<>();
                mp.put("a", randomString(10));
                mp.put("b", randomString(10));
                jedis.hmset(key, mp);
                if (i % 10000 == 0)
                    System.out.printf("%d operations done\n", i);
            }
            System.out.printf("%d elements inserted\n", ITERATIONS);
        }
    }

}
