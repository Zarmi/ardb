import org.apache.commons.lang3.RandomStringUtils;
import org.junit.*;
import org.junit.runners.MethodSorters;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static utils.TestUtils.*;

/**
 * Created by pva701 on 06.06.16.
 */

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StressTest {
    public static final int NUM_CLIENTS = 8;
    public static final int MAX_NUM_KEYS = 600_000;//with test config
    public static final int ITERATIONS = 1_000_000;
    public static final int MAX_SIZE_DATA_IN_BYTES = 6500;
    public static final int MIN_SIZE_DATA_IN_BYTES = 4300;

    private ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private Lock readLock = lock.readLock();
    private Lock writeLock = lock.writeLock();
    private String MD5_BASE;

    @BeforeClass
    public static void init() {
        flushAll();
    }

    @Test
    public void test1Performance() {
        System.out.println("Perfomance test");
        try (Jedis jedis = createJedis()) {
            for (int i = 1; i <= MAX_NUM_KEYS; ++i) {
                hmsetRandomMap(jedis);
                if (i % 10000 == 0)
                    System.out.printf("%d iterations done\n", i);
            }
            keys(jedis);
        }
    }

    @Test
    public void test2DeletingKeysWhenCompaction() throws Exception {
        System.out.println("DeletingKeysWhenCompaction test");

        int prob = 10000 * NUM_CLIENTS;
        System.out.printf("Probably: %d %.2f\n", prob, 1.0 / prob);
        MD5_BASE = RandomStringUtils.randomAscii(32);
        ExecutorService service = Executors.newFixedThreadPool(NUM_CLIENTS);
        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < NUM_CLIENTS; ++i)
            futures.add(service.submit(new Client(ITERATIONS,  prob, i)));
        for (Future f: futures)
            f.get();
        service.shutdown();
    }

    public class Client implements Runnable {
        public final int ITERATIONS;
        private final int myId;
        private final int probablyKEYSOperation;

        public Client(int iterations, int probablyKEYSOperation, int myId) {
            ITERATIONS = iterations;
            this.myId = myId;
            this.probablyKEYSOperation = probablyKEYSOperation;
        }

        @Override
        public void run() {
            System.out.println(String.format("Start client #%d", myId));
            Random random = new Random(myId);
            try (Jedis jedis = createJedis()) {
                for (int i = 1; i <= ITERATIONS; ++i) {
                    if (random.nextInt(probablyKEYSOperation) == 0) {
                        //Call keys
                        System.out.printf("Client %d: KEYS operation\n", myId);
                        writeLock.lock();
                        try {
                            keys(jedis);
                            //MD5_BASE = RandomStringUtils.randomAscii(32);
                        } finally {
                            writeLock.unlock();
                        }
                    } else {
                        readLock.lock();
                        try {
                            hmsetRandomMap(jedis);
                        } finally {
                            readLock.unlock();
                        }
                    }
                    if (i % 10000 == 0)
                        System.out.printf("Client %d: %d iterations done\n", myId, i);
                }
            }
        }
    }

    private void keys(Jedis jedis) {
        long startOp = System.currentTimeMillis();
        Set<String> keys = jedis.keys("*" + MD5_BASE + "*");
        Assert.assertTrue(keys.size() <= MAX_NUM_KEYS);
        System.out.println(String.format("KEYS *%s*: got %d keys, with time %.2f sec", MD5_BASE, keys.size(), (System.currentTimeMillis() - startOp) / 1000.0));
    }

    private void hmsetRandomMap(Jedis jedis) {
        Map<String, String> mp = genRandomMap("id", "key", "data");
        int valueLen = random.nextInt(MAX_SIZE_DATA_IN_BYTES - MIN_SIZE_DATA_IN_BYTES) + MIN_SIZE_DATA_IN_BYTES;
        int keyLen1 = random.nextInt(5) + 10;
        int keyLen2 = random.nextInt(5) + 10;
        mp.put("data", RandomStringUtils.randomAscii(valueLen));
        String key = RandomStringUtils.randomAscii(keyLen1) + MD5_BASE + RandomStringUtils.randomAscii(keyLen2);
        jedis.hmset(key, mp);
    }

    @After
    public void afterTest() {
        flushAll();
    }

}
