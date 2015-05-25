package org.neo4j.elasticsearch;

import static org.junit.Assert.*;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;

import java.io.Serializable;
import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.TestGraphDatabaseFactory;


public class ElasticSearchImportAppTest {

    private GraphDatabaseAPI db;
    private GraphDatabaseShellServer server;
    private SameJvmClient neo4jClient;
    private JestClient esClient;
    
    public static final String LABEL = "MyLabel";
    public static final String INDEX = "my_index";
    public static final String INDEX_SPEC = INDEX + ":" + LABEL + "(foo,bar)";

    @Before
    public void setUp() throws Exception {

        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .newGraphDatabase();
        
        server = new GraphDatabaseShellServer(db);
        neo4jClient = new SameJvmClient(Collections.<String, Serializable>emptyMap(), server);
        
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .multiThreaded(true)
                .build());
        esClient = factory.getObject();

        // create index
        esClient.execute(new CreateIndex.Builder(INDEX).build());
    }

    @After
    public void tearDown() throws Exception {
        esClient.execute(new DeleteIndex.Builder(INDEX).build());
        esClient.shutdownClient();
        db.shutdown();
    }
    
    @Test
    public void testImportCommand() throws Exception {
        int dataItems = 100;
        createTestData(dataItems);
        neo4jClient.evaluate("elasticsearch-index -s " + INDEX_SPEC);
        
        Thread.sleep(2000);
        
        String query = "{\"query\":{\"bool\":{\"must\":[{\"wildcard\":{\"MyLabel.foo\":\"*value*\"}}],\"must_not\":[],\"should\":[]}},\"from\":0,\"size\":10,\"sort\":[],\"facets\":{}}";
       
        Search search = new Search.Builder(query)
                                        .addIndex(INDEX)
                                        .build();

        SearchResult result = esClient.execute(search);
        
        assertEquals(new Integer(dataItems), result.getTotal());
    }
    
    @Test
    public void testBadSyntaxShouldThrow() throws Exception {
        neo4jClient.evaluate("elasticsearch-index -s index_name:Label(foo,bar");
    }
    
    private void createTestData(int items) {
        Transaction tx = db.beginTx();
        for (int i = 0; i < items; i++) {
            org.neo4j.graphdb.Node node = db.createNode(DynamicLabel.label(LABEL));
            node.setProperty("foo",String.format("foo_value_%s", i));
            node.setProperty("bar",String.format("bar_value_%s", i));
        }
        
        tx.success();tx.close();
        
        
    }
    
    
    
}
