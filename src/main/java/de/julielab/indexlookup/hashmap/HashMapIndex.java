package de.julielab.indexlookup.hashmap;

import de.julielab.indexlookup.StringIndex;

import java.util.HashMap;

public class HashMapIndex implements StringIndex {
    private HashMap<String, String> stringMap = new HashMap<>();
    private HashMap<String, String[]> stringArrayMap = new HashMap<>();

    @Override
    public String get(String key) {
        return stringMap.get(key);
    }

    @Override
    public String[] getArray(String key) {
        return stringArrayMap.get(key);
    }

    @Override
    public void put(String key, String value) {
        stringMap.put(key, value);
    }

    @Override
    public void put(String key, String[] value) {
        stringArrayMap.put(key, value);
    }

    @Override
    public void commit() {
        // nothing to do
    }

    @Override
    public boolean requiresExplicitCommit() {
        return false;
    }

    @Override
    public void close() {
        // nothing to do
    }

    @Override
    public void open() {
        // nothing to do
    }
}
