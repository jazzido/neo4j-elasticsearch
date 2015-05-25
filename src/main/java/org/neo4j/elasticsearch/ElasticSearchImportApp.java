package org.neo4j.elasticsearch;

import org.neo4j.shell.AppCommandParser;
import org.neo4j.shell.Continuation;
import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.impl.AbstractApp;
import org.neo4j.shell.kernel.GraphDatabaseShellServer;
import org.neo4j.helpers.Service;
import org.neo4j.shell.App;

//@Service.Implementation(App.class)
public class ElasticSearchImportApp extends AbstractApp {

    @Override
    public Continuation execute(AppCommandParser parser, Session session, Output out) throws Exception {
        // out.println("Caca");
        return Continuation.INPUT_COMPLETE;
    }
    
    @Override
    public String getName() {
        return "elasticsearch-index";
    }
    
    @Override
    public String getDescription()
    {
        return "Import index specs to ElasticSearch";
    }

    @Override
    public GraphDatabaseShellServer getServer() {
        return (GraphDatabaseShellServer) super.getServer();
    }
    
}
