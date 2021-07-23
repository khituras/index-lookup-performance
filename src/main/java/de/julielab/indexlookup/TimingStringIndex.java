package de.julielab.indexlookup;

public class TimingStringIndex implements StringIndex{
    private StringIndex index;
    private long timeForStringLookups = 0;
    private long timeForArrayLookups = 0;

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

    public void put(String key, String value) {
        index.put(key, value);
    }

    public void put(String key, String[] value) {
        index.put(key, value);
    }

    @Override
    public void commit() {
        index.commit();
    }

    @Override
    public boolean requiresExplicitCommit() {
        return index.requiresExplicitCommit();
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
}
