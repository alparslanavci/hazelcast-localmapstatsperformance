package com.hazelcast.localmapstatsperformance;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import org.openjdk.jmh.annotations.*;

import java.util.Random;
import java.util.logging.Logger;

/**
 * Created by alparslan on 15.09.2014.
 */
@State(value = Scope.Thread)
@OperationsPerInvocation(GetLocalStatsBenchmark.OPERATIONS_PER_INVOCATION)
public class GetLocalStatsBenchmark {
    public static final int OPERATIONS_PER_INVOCATION = 1000;
    private static final String MAP100_NAME = "someMap100";
    private static final String MAP1000_NAME = "someMap1000";
    private static final String MAP10000_NAME = "someMap10000";
    private static final String MAP100000_NAME = "someMap100000";
    private HazelcastInstance hz;
    private IMap<Integer, String> map100;
    private IMap<Integer, String> map1000;
    private IMap<Integer, String> map10000;
    private IMap<Integer, String> map100000;
    private Logger logger = Logger.getLogger(getClass().getName());

    @Setup
    public void setUp() {
        hz = Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();

        map100 = hz.getMap(MAP100_NAME);
        map1000 = hz.getMap(MAP1000_NAME);
        map10000 = hz.getMap(MAP10000_NAME);
        map100000 = hz.getMap(MAP100000_NAME);

        int map100Size = 100;
        int map1000Size = 1000;
        int map10000Size = 10000;
        int map100000Size = 100000;

        logger.info("Initializing map100 map...");
        for (int k = 0; k < map100Size; k++) {
            this.map100.put(k, randomString(50));
        }
        logger.info("map100 initialized.");

        logger.info("Initializing map1000 map...");
        for (int k = 0; k < map1000Size; k++) {
            this.map1000.put(k, randomString(50));
            if (k%1000==0){
                logger.info("map1000 count: " + this.map1000.size());
            }
        }
        logger.info("map1000 initialized.");

        logger.info("Initializing map10000 map...");
        for (int k = 0; k < map10000Size; k++) {
            this.map10000.put(k, randomString(50));
            if (k%1000==0){
                logger.info("map10000 count: " + this.map10000.size());
            }
        }
        logger.info("map10000 initialized.");

        logger.info("Initializing map100000 map...");
        for (int k = 0; k < map100000Size; k++) {
            this.map100000.put(k, randomString(50));
            if (k%1000==0){
                logger.info("map100000 count: " + this.map100000.size());
            }
        }
        logger.info("map100000 initialized.");
    }

    @TearDown
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    private String randomString(int length) {
        StringBuilder sb = new StringBuilder();
        Random random = new Random();
        for (int k = 0; k < length; k++) {
            char c = (char) random.nextInt(Character.MAX_VALUE);
            sb.append(c);
        }
        return sb.toString();
    }

    @GenerateMicroBenchmark
    public void map100GetLocalStatsPerformance() {
        for (int k = 0; k < OPERATIONS_PER_INVOCATION; k++) {
            map100.getLocalMapStats();
        }
    }

    @GenerateMicroBenchmark
    public void map1000GetLocalStatsPerformance() {
        for (int k = 0; k < OPERATIONS_PER_INVOCATION; k++) {
            map1000.getLocalMapStats();
        }
    }

    @GenerateMicroBenchmark
    public void map10000GetLocalStatsPerformance() {
        for (int k = 0; k < OPERATIONS_PER_INVOCATION; k++) {
            map10000.getLocalMapStats();
        }
    }

    @GenerateMicroBenchmark
    public void map100000GetLocalStatsPerformance() {
        for (int k = 0; k < OPERATIONS_PER_INVOCATION; k++) {
            map100000.getLocalMapStats();
        }
    }

}