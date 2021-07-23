package de.julielab.indexlookup.sqlite;

import de.julielab.indexlookup.StringIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SQLiteIndex implements StringIndex {
    private final static Logger log = LoggerFactory.getLogger(SQLiteIndex.class);
    private final PreparedStatement stringInsertionPs;
    private final PreparedStatement stringQueryPs;
    private final Statement stmt;
    private Connection connection;

    public SQLiteIndex() {
        connection = null;
        new File("test.db").deleteOnExit();
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:test.db");
            connection.setAutoCommit(false);
            stmt = connection.createStatement();
            stmt.execute("CREATE TABLE stringtable " +
                    "(key            TEXT    NOT NULL," +
                    " value          TEXT    NOT NULL)");
            stmt.execute("CREATE INDEX stringtable_index ON stringtable (key)");
            stringInsertionPs = connection.prepareStatement("INSERT INTO stringtable (key, value) VALUES (?,?)");
            stringQueryPs = connection.prepareStatement("SELECT value FROM stringtable WHERE key=?");
        } catch (Exception e) {
            log.error("Could connect to SQLite DB or create the table(s)", e);
            throw new IllegalStateException(e);
        }
        log.info("Opened SQLite database successfully");
    }

    @Override
    public String get(String key) {
        try {
//            stringQueryPs.setString(1, key);
//            stringQueryPs.addBatch();
//            ResultSet rs = stringInsertionPs.executeQuery();
            ResultSet rs = stmt.executeQuery("SELECT value FROM stringtable WHERE key='" + key + "'");
            return rs.next() ? rs.getString(1) : null;
        } catch (SQLException e) {
            log.error("Could not query key '{}' from the SQLite DB.", key, e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String[] getArray(String key) {
        List<String> ret = new ArrayList<>();
        try {
            stringQueryPs.setString(1, key);
            ResultSet rs = stringInsertionPs.executeQuery();
            while (rs.next()) {
                ret.add(rs.getString(1));
            }
        } catch (SQLException e) {
            log.error("Could not query key '{}' from the SQLite DB.", key, e);
            throw new IllegalStateException(e);
        }
        return ret.toArray(String[]::new);
    }

    @Override
    public void put(String key, String value) {
        try {
            stringInsertionPs.setString(1, key);
            stringInsertionPs.setString(2, value);
            stringInsertionPs.addBatch();
        } catch (SQLException e) {
            log.error("Could not add key-value pair {}:{} to the SQLite DB.", key, value);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void put(String key, String[] value) {
        for (var v : value)
            put(key, v);
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
}
