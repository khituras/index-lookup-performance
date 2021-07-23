package de.julielab.indexlookup.sql;

import de.julielab.indexlookup.StringIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.rowset.serial.SerialBlob;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SQLiteIndex implements StringIndex {
    private static final int BATCH_SIZE = 1000;
    private final static Logger log = LoggerFactory.getLogger(SQLiteIndex.class);
    private final PreparedStatement stringInsertionPs;
    private Statement stmt;
    private Connection connection;
    private int numPuts = 0;

    public SQLiteIndex() {
        open();
        try {
            stmt.execute("CREATE TABLE stringtable " +
                    "(key            TEXT    NOT NULL," +
                    " value          BLOB    NOT NULL)");
            stmt.execute("CREATE INDEX stringtable_index ON stringtable (key)");
            stringInsertionPs = connection.prepareStatement("INSERT INTO stringtable (key, value) VALUES (?,?)");
        } catch (Exception e) {
            log.error("Could connect to SQLite DB or create the table(s)", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String get(String key) {
        try {
            ResultSet rs = stmt.executeQuery("SELECT value FROM stringtable WHERE key='" + key + "'");
//            return rs.next() ? new String(rs.getBytes(1),StandardCharsets.UTF_8) : null;
            return rs.next() ? rs.getString(1) : null;
        } catch (SQLException e) {
            log.error("Could not query key '{}' from the SQLite DB.", key, e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String[] getArray(String key) {
        return get(key).split("\\$\\$");
//        List<String> ret = new ArrayList<>();
//        try {
//            ResultSet rs = stmt.executeQuery("SELECT value FROM stringtable WHERE key='" + key + "'");
//            while (rs.next()) {
//                ret.add(rs.getString(1));
//            }
//        } catch (SQLException e) {
//            log.error("Could not query key '{}' from the SQLite DB.", key, e);
//            throw new IllegalStateException(e);
//        }
//        return ret.toArray(String[]::new);
    }

    @Override
    public void put(String key, String value) {
        try {
            stringInsertionPs.setString(1, key);
//            stringInsertionPs.setBytes(2, value.getBytes(StandardCharsets.UTF_8));
            stringInsertionPs.setString(2, value);
            stringInsertionPs.addBatch();
            ++numPuts;
            if (numPuts >= BATCH_SIZE) {
                stringInsertionPs.executeBatch();
                numPuts = 0;
            }
        } catch (SQLException e) {
            log.error("Could not add key-value pair {}:{} to the SQLite DB.", key, value);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void put(String key, String[] value) {
        put(key, Stream.of(value).collect(Collectors.joining("$$")));
//        for (var v : value)
//            put(key, v);
    }

    @Override
    public void commit() {
        try {
            stringInsertionPs.executeBatch();
            connection.commit();
        } catch (SQLException e) {
            log.error("Could not commit to the SQLite database.", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean requiresExplicitCommit() {
        return true;
    }

    @Override
    public void close() {
        try {
            connection.close();
        } catch (SQLException e) {
            log.error("Could not close connection so SQLite.", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void open() {
        connection = null;
        String dbName = "sqlite";
        new File(dbName).deleteOnExit();
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:" + dbName);
            connection.setAutoCommit(false);
            stmt = connection.createStatement();
        } catch (Exception e) {
            log.error("Could not connect to SQLite DB.", e);
            throw new IllegalStateException(e);
        }
    }
}
