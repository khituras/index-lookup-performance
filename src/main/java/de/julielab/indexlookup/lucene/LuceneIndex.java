package de.julielab.indexlookup.lucene;

import de.julielab.indexlookup.StringIndex;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

public class LuceneIndex implements StringIndex {
    private final static Logger log = LoggerFactory.getLogger(LuceneIndex.class);
    private final IndexWriter iw;
    private final FSDirectory directory;
    private IndexSearcher searcher;

    public LuceneIndex() {
        try {
            Path lucene = Path.of("lucene");
            lucene.toFile().deleteOnExit();
            directory = NIOFSDirectory.open(lucene);
            IndexWriterConfig iwc = new IndexWriterConfig();
            iw = new IndexWriter(directory, iwc);
        } catch (IOException e) {
            log.error("could not initialize Lucene index", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String get(String key) {
        TermQuery tq = new TermQuery(new Term("key", key));
        BooleanQuery.Builder b = new BooleanQuery.Builder();
        b.add(tq, BooleanClause.Occur.FILTER);
        BooleanQuery q = b.build();
        try {
            TopDocs topDocs = searcher.search(q, 1);
            if (topDocs.scoreDocs.length > 0) {
                Document doc = searcher.getIndexReader().document(topDocs.scoreDocs[0].doc);
                return doc.getField("value").stringValue();
            }
        } catch (IOException e) {
            log.error("Could not retrieve results for '{}' in Lucene index.", key, e);
            throw new IllegalStateException(e);
        }
        return null;
    }

    @Override
    public String[] getArray(String key) {
        TermQuery tq = new TermQuery(new Term("key", key));
        BooleanQuery.Builder b = new BooleanQuery.Builder();
        b.add(tq, BooleanClause.Occur.FILTER);
        BooleanQuery q = b.build();
        try {
            TopDocs topDocs = searcher.search(q, 1);
            if (topDocs.scoreDocs.length > 0) {
                Document doc = searcher.getIndexReader().document(topDocs.scoreDocs[0].doc);
                return Arrays.stream(doc.getFields("value")).map(IndexableField::stringValue).toArray(String[]::new);
            }
        } catch (IOException e) {
            log.error("Could not retrieve results for '{}' in Lucene index.", key, e);
            throw new IllegalStateException(e);
        }
        return null;
    }

    @Override
    public void put(String key, String value) {
        Field keyField = new StringField("key", key, Field.Store.NO);
        Field valueField = new StoredField("value", value);
        Document doc = new Document();
        doc.add(keyField);
        doc.add(valueField);
        try {
            iw.addDocument(doc);
        } catch (IOException e) {
            log.error("Could not index key-value pair {}:{} with Lucene", key, value, e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void put(String key, String[] value) {
        Field keyField = new StringField("key", key, Field.Store.NO);
        Document doc = new Document();
        doc.add(keyField);
        for (var v : value)
            doc.add(new StoredField("value", v));
        try {
            iw.addDocument(doc);
        } catch (IOException e) {
            log.error("Could not index key-value pair {}:{} with Lucene", key, value, e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void commit() {
        try {
            iw.commit();
        } catch (IOException e) {
            log.error("Could not commit Lucene index", e);
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
            if (searcher != null)
                searcher.getIndexReader().close();
        } catch (IOException e) {
            log.error("Could not close Lucene index reader.", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void open() {
        try {
            searcher = new IndexSearcher(DirectoryReader.open(directory));
        } catch (IOException e) {
            log.error("Could not open Lucene index searcher.", e);
            throw new IllegalStateException(e);
        }
    }
}
