package Controllers;

import CreateDB.Bootstrappers.BootstrapNeo4j;
import DatabaseHelper.QueryHelper;
import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.ExecutionResult;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;

import java.io.IOException;
import java.util.*;

public class GraphController {
    private GraphDatabaseService graphDb = null;

    // Query files
    static final String reachabilityCypherFileName = "Graph/Reachability.cql";
    static final String neighborCountCypherFileName = "Graph/NeighborCount.cql";

    public GraphController() { }

    public void loadGraphDB(String sourcePath) throws IOException {
        BootstrapNeo4j.bootstrap(sourcePath);
    }

    public GraphDatabaseService getGraphDatabaseService() throws IOException {
        if (this.graphDb == null) {
            this.graphDb = BootstrapNeo4j.getDatabaseService();
        }
        return this.graphDb;
    }

    public void finish() {
        if (this.graphDb != null)
            this.graphDb.shutdown();
    }

    public static void executeReachabilityCountCypher(GraphDatabaseService graphDb,
                                                      int queryParameter) throws IOException {
        String statementStr = QueryHelper.getQueryStatementFromFile(reachabilityCypherFileName);
        ExecutionEngine engine = new ExecutionEngine(graphDb);
        Map<String, Object> params = new HashMap<>();
        params.put("value", queryParameter);
        ExecutionResult result = engine.execute(statementStr, params);
        System.out.println(result.dumpToString());
    }

    public static Iterator<Map<String, Object>> executeNeighborCountCypher(GraphDatabaseService graphDb,
                                                  int queryParameter) throws IOException {
        String statementStr = QueryHelper.getQueryStatementFromFile(neighborCountCypherFileName);
        ExecutionEngine engine = new ExecutionEngine(graphDb);
        Map <String, Object> params = new HashMap<>();
        params.put("value", queryParameter);
        ExecutionResult executionResult = engine.execute(statementStr, params);
        ResourceIterator<Map<String,Object>> resultIter = executionResult.iterator();

        List<Map<String, Object>> resultList = new LinkedList<>();
        while (resultIter.hasNext()) {
            resultList.add(resultIter.next());
        }
        resultIter.close();

        return resultList.iterator();
    }

    // Alap
    public static Map<String, Object> reachabilityTraversal(GraphDatabaseService graphDb, int queryParameter) {
        int neighbors = 0;

        try (Transaction tx = graphDb.beginTx()) {
            Node node = graphDb.getNodeById(queryParameter);
            TraversalDescription td = graphDb.traversalDescription()
                    .breadthFirst()
                    .relationships(BootstrapNeo4j.getConnectedRelationship(), Direction.BOTH)
                    .evaluator(Evaluators.excludeStartPosition());
                    //.evaluator(Evaluators.toDepth(500));
            for (Path reachableNode : td.traverse(node)) {
                neighbors++;
            }
            tx.success();
        }

        Map<String, Object> result = new HashMap<>();
        result.put("ReachabilityCount", neighbors);
        return result;
    }
}
