package de.julielab.indexlookup.mapdb;

import de.julielab.indexlookup.StringIndex;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.File;

public class MapDbIndex implements StringIndex {

    private HTreeMap<String, String[]> arrayIndex;
    private HTreeMap<String, String> stringIndex;
    private DB filedb;

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

    @Override
    public void open() {
        String dbPath = "mapdb.db";
        File dbFile = new File(dbPath);
        dbFile.deleteOnExit();
        filedb = DBMaker.fileDB(dbFile).fileMmapEnableIfSupported().cleanerHackEnable().make();
        stringIndex = filedb.hashMap("StringIndex").
                keySerializer(Serializer.STRING).valueSerializer(Serializer.STRING).
                createOrOpen();
        arrayIndex = filedb.hashMap("ArrayIndex").
                keySerializer(Serializer.STRING).valueSerializer(Serializer.JAVA).
                createOrOpen();
    }
}
