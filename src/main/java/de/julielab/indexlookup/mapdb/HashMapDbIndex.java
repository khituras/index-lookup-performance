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
    public void open() {
        String dbPath = "hashmapdb.db";
        File dbFile = new File(dbPath);
        dbFile.deleteOnExit();
        filedb = DBMaker.fileDB(dbFile).fileMmapEnableIfSupported().cleanerHackEnable().make();
        stringIndex = filedb.hashMap("StringIndex").
                keySerializer(Serializer.STRING).valueSerializer(Serializer.BYTE_ARRAY).
                createOrOpen();
    }
}
