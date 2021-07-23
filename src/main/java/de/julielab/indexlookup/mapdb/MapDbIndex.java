package de.julielab.indexlookup.mapdb;

import de.julielab.indexlookup.StringIndex;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.File;
import java.util.Map;

public abstract class MapDbIndex implements StringIndex {

    protected Map<String, String[]> arrayIndex;
    protected Map<String, String> stringIndex;
    protected DB filedb;

    public MapDbIndex() {
       open();
    }

    @Override
    public String get(String key) {
        return stringIndex.get(key);
    }

    @Override
    public String[] getArray(String key) {
        return arrayIndex.get(key);
    }

    @Override
    public void put(String key, String value) {
        stringIndex.put(key, value);
    }

    @Override
    public void put(String key, String[] value) {
        arrayIndex.put(key, value);
    }

    @Override
    public void commit() {
        filedb.commit();
    }

    @Override
    public boolean requiresExplicitCommit() {
        return true;
    }

    @Override
    public void close() {
        filedb.close();
    }

}
