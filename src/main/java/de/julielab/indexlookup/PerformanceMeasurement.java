package de.julielab.indexlookup;

import de.julielab.indexlookup.lucene.LuceneIndex;
import de.julielab.indexlookup.mapdb.HashMapDbIndex;
import de.julielab.indexlookup.mapdb.TreeMapDbIndex;
import de.julielab.indexlookup.sql.SQLiteIndex;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class PerformanceMeasurement {
    public static final int NUM_INDEX_ENTRIES = 10000000;
    public static final int NUM_QUERY_ENTRIES = 100000;
    public static final int ROUNDS = 3;
    public static final int MIN_VALUE_LENGTH = 2;
    public static final int MAX_KEY_LENGTH = 10;
    public static final int MAX_VALUE_LENGTH = 15;
    private final static Logger log = LoggerFactory.getLogger(PerformanceMeasurement.class);
    private List<TimingStringIndex> indices = new ArrayList<>();
    private Set<String> singleStringKeys = new HashSet<>(NUM_INDEX_ENTRIES);
    private Set<String> arrayStringKeys = new HashSet<>(NUM_INDEX_ENTRIES);
    private Random numValuesRandom = new Random(1);
    private Random subsetRandom;

    public PerformanceMeasurement() {
        indices.add(new TimingStringIndex(new SQLiteIndex()));
        indices.add(new TimingStringIndex(new LuceneIndex()));
        indices.add(new TimingStringIndex(new HashMapDbIndex()));
        indices.add(new TimingStringIndex(new TreeMapDbIndex()));
//        indices.add(new TimingStringIndex(new HashMapIndex()));
        subsetRandom = new Random(1);
    }

    public static void main(String args[]) {
        PerformanceMeasurement performanceMeasurement = new PerformanceMeasurement();
        performanceMeasurement.run();
    }

    private void run() {
        index();
        log.info("Data indexing has finished.");
        for (var index : indices) {
            log.info("Index {} took {}s for single string indexing", index.getName(), index.getTimeForStringIndexing() / Math.pow(10, 9));
            log.info("Index {} took {}s for array string indexing", index.getName(), index.getTimeForArrayIndexing() / Math.pow(10, 9));
        }
        log.info("xxx");
        for (var index : indices)
            index.close();
        Map<String, Long> time = new LinkedHashMap<>();
        for (int i = 0; i < ROUNDS; i++) {
            for (var index : indices) {
                index.resetTimings();
                index.open();
            }
            query();
            for (var index : indices) {
                long timeForStringLookups = index.getTimeForStringLookups();
                time.merge(index.getName(), timeForStringLookups, Long::sum);
                log.info("Index {} took {}s for single string lookups", index.getName(), timeForStringLookups / Math.pow(10, 9));
                long timeForArrayLookups = index.getTimeForArrayLookups();
                time.merge(index.getName(), timeForArrayLookups, Long::sum);
                log.info("Index {} took {}s for string array lookups", index.getName(), timeForArrayLookups / Math.pow(10, 9));
            }
            log.info("---");
            for (var index : indices)
                index.close();
        }
        log.info("===");
        for (var indexName : time.keySet()) {
            log.info("Index {} took an average of {}s over {} rounds", indexName, (time.get(indexName) / (double) ROUNDS) / Math.pow(10, 9), ROUNDS);
        }
    }

    private List<String> drawRandomSubset(Collection<String> from, int howMany) {
        List<String> randomKeySubset = new ArrayList<>(howMany);
        while (randomKeySubset.size() < howMany) {
            Iterator<String> it = from.iterator();
            while (it.hasNext() && randomKeySubset.size() < howMany) {
                String item = it.next();
                if (subsetRandom.nextBoolean())
                    randomKeySubset.add(item);
            }
        }
        return randomKeySubset;
    }

    private void query() {
        for (var key : drawRandomSubset(singleStringKeys, NUM_QUERY_ENTRIES)) {
            for (var index : indices) {
                String ignore = index.get(key);
                assert ignore != null : "A null value was returned for key '" + key + "' from index " + index.getName();
            }
        }
        for (var key : drawRandomSubset(arrayStringKeys, NUM_QUERY_ENTRIES)) {
            for (var index : indices) {
                String[] ignore = index.getArray(key);
                assert ignore != null : "A null value was returned for key '" + key + "' from index " + index.getName();
            }
        }
    }

    private void index() {
        for (int i = 0; i < NUM_INDEX_ENTRIES; i++) {
            indexSingleString();
            indexStringArray();
            if (i % (NUM_INDEX_ENTRIES / 10) == 0)
                log.info("Indexed {} items.", i);
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
        int numValues = numValuesRandom.nextInt(45) + 5;
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
