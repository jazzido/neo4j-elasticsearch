package org.neo4j.elasticsearch;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.DeleteIndex;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.test.TestGraphDatabaseFactory;


public class ElasticSearchImportAppTest {

    private JestClient client;
    private GraphDatabaseAPI db;
    private SameJvmClient neo4jClient;
    
    public static final String LABEL = "MyLabel";
    public static final String INDEX = "my_index";
    public static final String INDEX_SPEC = INDEX + ":" + LABEL + "(foo,bar)";

    @Before
    public void setUp() throws Exception {
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder("http://localhost:9200")
                .build());
        client = factory.getObject();
        db = (GraphDatabaseAPI) new TestGraphDatabaseFactory()
                .newImpermanentDatabaseBuilder()
                .setConfig(config())
                .newGraphDatabase();
        neo4jClient = new SameJvmClient(Collections.<String, Serializable>emptyMap(), new GraphDatabaseShellServer((GraphDatabaseAPI) db));

        // create index
        client.execute(new CreateIndex.Builder(INDEX).build());
    }

    private Map<String, String> config() {
        return stringMap(
                "elasticsearch.host_name", "http://localhost:9200",
                "elasticsearch.index_spec", INDEX_SPEC);
    }

    @After
    public void tearDown() throws Exception {
        client.execute(new DeleteIndex.Builder(INDEX).build());
        client.shutdownClient();
        db.shutdown();
    }
    
    @Test
    public void testImportCommand() throws ShellException {
        neo4jClient.evaluate("elasticsearch-index");
    }
    
    
    
}
