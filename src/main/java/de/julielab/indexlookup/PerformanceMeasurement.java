package de.julielab.indexlookup;

import de.julielab.indexlookup.hashmap.HashMapIndex;
import de.julielab.indexlookup.sqlite.SQLiteIndex;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class PerformanceMeasurement {
    public static final int NUM_ENTRIES = 100;
    public static final int MIN_KEY_LENGTH = 2;
    public static final int MIN_VALUE_LENGTH = 2;
    public static final int MAX_KEY_LENGTH = 10;
    public static final int MAX_VALUE_LENGTH = 15;
    private final static Logger log = LoggerFactory.getLogger(PerformanceMeasurement.class);
    private final Random keyLength;
    private final Random valueLength;
    private List<TimingStringIndex> indices = new ArrayList<>();
    private List<String> keys = new ArrayList<>(NUM_ENTRIES);
    private int seed = 0;

    public PerformanceMeasurement() {
        indices.add(new TimingStringIndex(new SQLiteIndex()));
        indices.add(new TimingStringIndex(new HashMapIndex()));
        keyLength = new Random(seed++);
        valueLength = new Random(seed++);
    }

    public static void main(String args[]) {
        PerformanceMeasurement performanceMeasurement = new PerformanceMeasurement();
        performanceMeasurement.run();
    }

    private void run() {
        index();
        query();
        for (var index : indices) {
            log.info("Index {} took {}s", index.getName(), index.getTimeForStringLookups() / Math.pow(10, 9));
        }
    }

    private void query() {
        for (var key : keys) {
            for (var index : indices) {
                String ignore = index.get(key);
                assert ignore != null : "A null value was returned for key '" + key + "' from index " + index.getName();
            }
        }
    }

    private void index() {
        for (int i = 0; i < NUM_ENTRIES; i++) {
            String key = RandomStringUtils.randomAlphanumeric(MAX_KEY_LENGTH, MAX_KEY_LENGTH);
            String value = RandomStringUtils.randomAlphanumeric(MIN_VALUE_LENGTH, MAX_VALUE_LENGTH);
            for (var index : indices) {
                index.put(key, value);
                keys.add(key);
            }
        }
        for (var index : indices) {
            if (index.requiresExplicitCommit())
                index.commit();
        }
    }
}
