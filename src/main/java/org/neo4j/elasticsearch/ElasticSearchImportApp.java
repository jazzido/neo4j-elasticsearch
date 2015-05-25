package org.neo4j.elasticsearch;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Bulk;
import io.searchbox.core.Bulk.Builder;
import io.searchbox.core.Index;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.OptionDefinition;
import org.neo4j.shell.OptionValueType;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.impl.AbstractApp;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Result;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.shell.App;

@Service.Implementation(App.class)
public class ElasticSearchImportApp extends AbstractApp {
    
    private final String ES_DEFAULT_HOST = "http://localhost:9200";
    private final String ID_COLUMN_ALIAS = "__node_id";
    private final String LABELS_COLUMN_ALIAS = "__node_labels";
    private JestClient esClient;
    
    {
        addOptionDefinition( "s", new OptionDefinition( OptionValueType.MUST,
                "Indexing specification (eg: people:Person(first_name,last_name)" ) );
        addOptionDefinition( "h", new OptionDefinition( OptionValueType.MAY,
                "ElasticSearch host name. Default is http://localhost:9200"));
        
    }

    @Override
    public Continuation execute(AppCommandParser parser, Session session, Output out) throws Exception {
        
        GraphDatabaseAPI db = getServer().getDb();

        // setup ES client
        String esHost = parser.option("h", ES_DEFAULT_HOST);
        JestClientFactory factory = new JestClientFactory();
        factory.setHttpClientConfig(new HttpClientConfig
                .Builder(esHost)
                .build());
        esClient = factory.getObject();
        
        out.println(String.format("Connected to ES cluster: %s", esHost));
        
        Map<Label, List<ElasticSearchIndexSpec>> indexSpecs = ElasticSearchIndexSpecParser.parseIndexSpec(parser.option("s", null));        
        
        for (Map.Entry<Label, List<ElasticSearchIndexSpec>> e: indexSpecs.entrySet()) {
            for (ElasticSearchIndexSpec spec: e.getValue()) {
                String query = generateQuery(e.getKey(), spec);
                Result result = db.execute(query);
                Bulk reqs = indexRequests(result, spec.getIndexName(), e.getKey(), spec.getProperties());
                out.println(String.format("Indexing %s to %s", e.getKey().name(), spec.getIndexName()));
                esClient.execute(reqs);
            }
        }
        
        return Continuation.INPUT_COMPLETE;
    }
    
    @Override
    public String getName() {
        return "elasticsearch-index";
    }
    
    @Override
    public GraphDatabaseShellServer getServer()
    {
        return ( GraphDatabaseShellServer ) super.getServer();
    }
    
    @Override
    public String getDescription()
    {
        return "Import index specs to ElasticSearch";
    }
    
    
    private String generateQuery(Label label, ElasticSearchIndexSpec spec) {
        StringBuilder sb = new StringBuilder(
                String.format("MATCH (n:%s) RETURN id(n) as %s, labels(n) as %s,", 
                        label.name(), 
                        ID_COLUMN_ALIAS,
                        LABELS_COLUMN_ALIAS)
                );
        Boolean first = true;
        for (String property: spec.getProperties()) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(String.format("n.%s as %s", property, property));
            first = false;
        }
        return sb.toString();
    }
    
    private Bulk indexRequests(Result result, String index, Label label, Collection<String> properties) {
        
        Builder bulk = new Bulk.Builder().defaultIndex(index).defaultType(label.name());
      
        while (result.hasNext()) {
            Map<String, Object> row = result.next();
            bulk.addAction(new Index.Builder(rowToJson(row, properties)).id(String.valueOf(row.get(ID_COLUMN_ALIAS))).build());
        }
        
        return bulk.build();
    }

    private Map rowToJson(Map<String,Object> row, Collection<String> properties) {
        Map<String,Object> json = new LinkedHashMap<>();
        json.put("id", row.get(ID_COLUMN_ALIAS));
        json.put("labels", row.get(LABELS_COLUMN_ALIAS));
        for (String prop : properties) {
            Object value = row.get(prop);
            json.put(prop, value);
        }
        return json;
    }
    
    
}
