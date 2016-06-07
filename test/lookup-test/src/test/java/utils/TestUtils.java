package utils;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Assert;
import redis.clients.jedis.Jedis;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by pva701 on 06.06.16.
 */
public class TestUtils {
    public static Random random = new Random();
    public static final String[] fields = new String[] {"a", "b", "c"};

    public static Map<String, String> genRandomMap(String... fields) {
        Map<String, String> ret = new HashMap<>();
        for (String field: fields)
            ret.put(field, RandomStringUtils.randomAscii(random.nextInt(50) + 1));
        return ret;
    }


    public static Set<String> toSet(String[] a) {
        return new HashSet<>(Arrays.asList(a));
    }

    public static SortedSet<String> toSortedSet(String[] a) {
        return new TreeSet<>(Arrays.asList(a));
    }

    public static SortedSet<String> filter(Iterable<? extends String> set, String patternString) {
        patternString = patternString.replace("*", ".*").replace("?", ".");
        Pattern pattern = Pattern.compile("^" + patternString + "$");
        TreeSet<String> ret = new TreeSet<>();

        for (String x: set)
            if (pattern.matcher(x).find())
                ret.add(x);
        return ret;
    }

    public static void remove(Set<String> set, Jedis jedis, String key) {
        set.remove(key);
        jedis.del(key);
    }

    public static void add(Set<String> set, Jedis jedis, String key) {
        set.add(key);
        jedis.hmset(key, genRandomMap(fields));
    }

    public static void testPattern(Set<String> keys, Jedis jedis, String pattern) {
        Assert.assertEquals(filter(keys, pattern), jedis.keys(pattern));
    }

    public static List<String> toList(Set<String> set) {
        List<String> list = new ArrayList<>();
        list.addAll(set);
        return list;
    }

    public static void deleteAll() {
        long startTime = System.currentTimeMillis();
        final int numThread = 8;
        System.out.println("Parallel deleting start now, threads = " + numThread);

        Set<String> keysSet;
        try (Jedis jedis = createJedis()) {
             keysSet = jedis.keys("*");
        }

        List<String> keys = toList(keysSet);
        try {
            ExecutorService service = Executors.newFixedThreadPool(numThread);
            List<Future> futures = new ArrayList<>();
            int startIndex = 0;
            for (int i = 0; i < numThread; ++i) {
                final int left = startIndex;
                final int right = startIndex + keysSet.size() / numThread + (i < keysSet.size() % numThread ? 1 : 0);
                startIndex = right;
                final List<String> subList = keys.subList(left, right);

                futures.add(service.submit(() -> {
                    try (Jedis jedis = createJedis()) {
                        for (String x: subList)
                            jedis.del(x);
                    }
                }));
            }

            for (Future f : futures)
                f.get();

            try (Jedis jedis = createJedis()) {
                Assert.assertEquals(Collections.emptySet(), jedis.keys("*"));
            }

            service.shutdown();
            System.out.printf("Parallel deleting finished with time %.2f sec\n", (System.currentTimeMillis() - startTime) / 1000.0);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void flushAll() {
        try (Jedis jedis = createJedis()) {
            jedis.flushAll();
            Assert.assertEquals(Collections.emptySet(), jedis.keys("*"));
        }
    }

    public static Jedis createJedis() {
        return new Jedis("localhost", 16379, -1);
    }
}
