package de.julielab.indexlookup.mapdb;

import org.mapdb.DBMaker;
import org.mapdb.Serializer;

import java.io.File;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TreeMapDbIndex extends MapDbIndex {



    @Override
    public void open() {
        String dbPath = "treemapdb.db";
        File dbFile = new File(dbPath);
        dbFile.deleteOnExit();
        filedb = DBMaker.fileDB(dbFile).fileMmapEnableIfSupported().cleanerHackEnable().make();
        stringIndex = filedb.treeMap("StringIndex").
                keySerializer(Serializer.STRING).valueSerializer(Serializer.BYTE_ARRAY).
                createOrOpen();
    }
}
