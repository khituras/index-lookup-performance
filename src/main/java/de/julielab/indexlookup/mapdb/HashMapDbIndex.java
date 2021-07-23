package de.julielab.indexlookup.mapdb;

import de.julielab.indexlookup.StringIndex;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

import java.io.File;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HashMapDbIndex extends MapDbIndex {

    @Override
    public String[] getArray(String key) {
        return stringIndex.get(key).split("\\$\\$");
    }

    @Override
    public void put(String key, String[] value) {
        stringIndex.put(key, Stream.of(value).collect(Collectors.joining("$$")));
    }

    @Override
    public void open() {
        String dbPath = "hashmapdb.db";
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
