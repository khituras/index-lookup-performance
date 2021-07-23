package de.julielab.indexlookup;

public class TimingStringIndex implements StringIndex{
    private StringIndex index;
    private long timeForStringLookups = 0;
    private long timeForArrayLookups = 0;
    private long timeForStringIndexing = 0;
    private long timeForArrayIndexing = 0;

    public TimingStringIndex(StringIndex index) {
        this.index = index;
    }

    public String get(String key) {
        long time = System.nanoTime();
        try {
            return index.get(key);
        } finally {
            timeForStringLookups += System.nanoTime() - time;
        }
    }

    public String[] getArray(String key) {
        long time = System.nanoTime();
        try {
            return index.getArray(key);
        } finally {
            timeForArrayLookups += System.nanoTime() - time;
        }
    }

    public long getTimeForStringIndexing() {
        return timeForStringIndexing;
    }

    public long getTimeForArrayIndexing() {
        return timeForArrayIndexing;
    }

    public void put(String key, String value) {
        long time = System.nanoTime();
        index.put(key, value);
        timeForStringIndexing += System.nanoTime() - time;
    }

    public void put(String key, String[] value) {
        long time = System.nanoTime();
        index.put(key, value);
        timeForArrayIndexing += System.nanoTime() - time;
    }

    @Override
    public void commit() {
        long time = System.nanoTime();
        index.commit();
        time = System.nanoTime() - time;
        timeForStringIndexing += time;
        timeForArrayIndexing += time;
    }

    @Override
    public boolean requiresExplicitCommit() {
        return index.requiresExplicitCommit();
    }

    @Override
    public void close() {
        long time = System.nanoTime();
        index.close();
        time = System.nanoTime() - time;
        timeForStringIndexing += time;
        timeForArrayIndexing += time;
    }

    @Override
    public void open() {
        index.open();
    }

    @Override
    public String getName() {
        return index.getName();
    }

    public long getTimeForStringLookups() {
        return timeForStringLookups;
    }

    public long getTimeForArrayLookups() {
        return timeForArrayLookups;
    }

    public void resetTimings() {
        timeForStringLookups = 0;
        timeForArrayLookups = 0;
        timeForStringIndexing = 0;
        timeForArrayIndexing = 0;
    }
}
