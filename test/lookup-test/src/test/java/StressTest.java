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
    public static final int MAX_SIZE_DATA_IN_BYTES = 6500;
    public static final int MIN_SIZE_DATA_IN_BYTES = 4300;

    /*private ReadWriteLock lock = new ReentrantReadWriteLock(true);
    private Lock readLock = lock.readLock();
    private Lock writeLock = lock.writeLock();*/
    private volatile String MD5_BASE;

    @BeforeClass
    public static void init() {
        flushAll();
    }

    @Test
    public void test1Performance() {
        System.out.println("Perfomance test");
        MD5_BASE = randomString(32);
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
        final int ITERATIONS = 500_000;

        System.out.println("DeletingKeysWhenCompaction test");

        int prob = 10000 * NUM_CLIENTS;
        System.out.printf("Probably: %d %.2f\n", prob, 1.0 / prob);
        MD5_BASE = randomString(32);
        ExecutorService service = Executors.newFixedThreadPool(NUM_CLIENTS);
        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < NUM_CLIENTS; ++i)
            futures.add(service.submit(new ClientStreamOperations(ITERATIONS,  prob, i)));
        for (Future f: futures)
            f.get();
        service.shutdown();
    }

    @Test
    public void testSubstringReturn() throws Exception {
        final int numSubs = 50;
        final String[] subs = new String[numSubs];
        final Set<String>[] keys = new TreeSet[numSubs];
        for (int i = 0; i < numSubs; ++i) {
            keys[i] = new TreeSet<>();
            subs[i] = randomString(randInt(10, 30));
        }
        ExecutorService service = Executors.newFixedThreadPool(NUM_CLIENTS);
        final int ITERATIONS = 50000;
        int prob = 100;

        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < NUM_CLIENTS; ++i)
            futures.add(service.submit(new ClientSubstring(i, ITERATIONS,  prob, subs, keys)));
        for (Future f: futures)
            f.get();
        service.shutdown();
    }

    public class ClientSubstring implements Runnable {
        private final int ITERATIONS;
        private final int p;
        private final Set<String>[] keys;
        private final String[] subs;
        private final int myId;

        public ClientSubstring(int myId, int iterations, int p, String[] subs, Set<String>[] keys) {
            this.myId = myId;
            this.ITERATIONS = iterations;
            this.p = p;
            this.keys = keys;
            this.subs = subs;
        }


        @Override
        public void run() {
            try (Jedis jedis = createJedis()) {
                for (int i = 1; i <= ITERATIONS; ++i) {
                    if (random.nextInt(p) == 0) {
                        System.out.printf("Client %d: KEYS operation\n", myId);
                        int subId = randInt(subs.length);
                        String sub = subs[subId];
                        synchronized (keys[subId]) {
                            Set<String> cur = jedis.keys("*" + sub + "*");
                            Assert.assertEquals(keys[subId], cur);
                        }
                    } else {
                        int subId = randInt(subs.length);
                        String sub = subs[subId];
                        String key = randomString(randInt(10)) + sub + randomString(randInt(10));
                        synchronized (keys[subId]) {
                            add(keys[subId], jedis, key);
                            keys[subId].add(key);
                        }
                    }

                    if (i % 10000 == 0)
                        System.out.printf("ClientStreamOperations %d: %d iterations done\n", myId, i);
                }
            }
        }
    }


    public class ClientStreamOperations implements Runnable {
        public final int ITERATIONS;
        private final int myId;
        private final int probablyKEYSOperation;

        public ClientStreamOperations(int iterations, int probablyKEYSOperation, int myId) {
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
                        keys(jedis);
                        //MD5_BASE = RandomStringUtils.randomAscii(32);
                    } else
                        hmsetRandomMap(jedis);

                    if (i % 10000 == 0)
                        System.out.printf("ClientStreamOperations %d: %d iterations done\n", myId, i);
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
        mp.put("data", randomString(valueLen));
        String key = randomString(keyLen1) + MD5_BASE + randomString(keyLen2);
        jedis.hmset(key, mp);
    }

    @After
    public void afterTest() {
        flushAll();
    }

}
