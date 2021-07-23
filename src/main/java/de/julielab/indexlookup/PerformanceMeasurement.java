package de.julielab.indexlookup;

import de.julielab.indexlookup.hashmap.HashMapIndex;
import de.julielab.indexlookup.lucene.LuceneIndex;
import de.julielab.indexlookup.sqlite.SQLiteIndex;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PerformanceMeasurement {
    public static final int NUM_ENTRIES = 100000;
    public static final int ROUNDS = 3;
    public static final int MIN_VALUE_LENGTH = 2;
    public static final int MAX_KEY_LENGTH = 10;
    public static final int MAX_VALUE_LENGTH = 15;
    private final static Logger log = LoggerFactory.getLogger(PerformanceMeasurement.class);
    private List<TimingStringIndex> indices = new ArrayList<>();
    private Set<String> singleStringKeys = new HashSet<>(NUM_ENTRIES);
    private Set<String> arrayStringKeys = new HashSet<>(NUM_ENTRIES);
    private Random numValuesRandom = new Random(1);

    public PerformanceMeasurement() {
        indices.add(new TimingStringIndex(new SQLiteIndex()));
        indices.add(new TimingStringIndex(new HashMapIndex()));
        indices.add(new TimingStringIndex(new LuceneIndex()));
    }

    public static void main(String args[]) {
        PerformanceMeasurement performanceMeasurement = new PerformanceMeasurement();
        performanceMeasurement.run();
    }

    private void run() {
        index();
        Map<String, Long> time = new HashMap<>();
        for (int i = 0; i < ROUNDS; i++) {
            query();
            for (var index : indices) {
                long timeForStringLookups = index.getTimeForStringLookups();
                time.merge(index.getName(), timeForStringLookups, Long::sum);
                log.info("Index {} took {}s", index.getName(), timeForStringLookups / Math.pow(10, 9));
            }
            for (var index : indices) {
                long timeForArrayLookups = index.getTimeForArrayLookups();
                time.merge(index.getName(), timeForArrayLookups, Long::sum);
                log.info("Index {} took {}s", index.getName(), timeForArrayLookups / Math.pow(10, 9));
            }
        }
        for (var indexName : time.keySet()) {
            log.info("Index {} took an average of {}s over {} rounds", indexName, (time.get(indexName)/(double)ROUNDS) / Math.pow(10, 9), ROUNDS);
        }
    }

    private void query() {
        for (var key : singleStringKeys) {
            for (var index : indices) {
                String ignore = index.get(key);
                assert ignore != null : "A null value was returned for key '" + key + "' from index " + index.getName();
            }
        }
        for (var key : arrayStringKeys) {
            for (var index : indices) {
                String[] ignore = index.getArray(key);
                assert ignore != null : "A null value was returned for key '" + key + "' from index " + index.getName();
            }
        }
    }

    private void index() {
        for (int i = 0; i < NUM_ENTRIES; i++) {
            indexSingleString();
            indexStringArray();
        }
        for (var index : indices) {
            if (index.requiresExplicitCommit())
                index.commit();
        }
    }

    private void indexStringArray() {
        String arrayKey;
        do {
            arrayKey = RandomStringUtils.randomAlphanumeric(MAX_KEY_LENGTH, MAX_KEY_LENGTH);
        } while (singleStringKeys.contains(arrayKey));
        int numValues = numValuesRandom.nextInt(50);
        String[] valueArray = new String[numValues];
        for (int j = 0; j < valueArray.length; j++) {
            String v = RandomStringUtils.randomAlphanumeric(MIN_VALUE_LENGTH, MAX_VALUE_LENGTH);
            valueArray[j] = v;
        }
        for (var index : indices) {
            index.put(arrayKey, valueArray);
            arrayStringKeys.add(arrayKey);
        }
    }

    private void indexSingleString() {
        String key = RandomStringUtils.randomAlphanumeric(MAX_KEY_LENGTH, MAX_KEY_LENGTH);
        String value = RandomStringUtils.randomAlphanumeric(MIN_VALUE_LENGTH, MAX_VALUE_LENGTH);
        for (var index : indices) {
            index.put(key, value);
            singleStringKeys.add(key);
        }
    }
}
