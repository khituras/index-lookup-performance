package de.julielab.indexlookup;

public interface StringIndex {
    String get(String key);

    String[] getArray(String key);

    void put(String key, String value);

    void put(String key, String[] value);

    void commit();

    boolean requiresExplicitCommit();

    void close();

    void open();

    default String getName() {
        return getClass().getSimpleName();
    }
}
