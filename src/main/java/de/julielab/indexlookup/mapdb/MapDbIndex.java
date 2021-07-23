package de.julielab.indexlookup.mapdb;

import de.julielab.indexlookup.StringIndex;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class MapDbIndex implements StringIndex {

    protected Map<String, byte[]> stringIndex;
    protected DB filedb;

    public MapDbIndex() {
       open();
    }

    @Override
    public String[] getArray(String key) {
        return new String(stringIndex.get(key), StandardCharsets.UTF_8).split("\\$\\$");
    }

    @Override
    public void put(String key, String[] value) {
        stringIndex.put(key, Stream.of(value).collect(Collectors.joining("$$")).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String get(String key) {
        return new String(stringIndex.get(key), StandardCharsets.UTF_8);
    }


    @Override
    public void put(String key, String value) {
        stringIndex.put(key, value.getBytes(StandardCharsets.UTF_8));
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
