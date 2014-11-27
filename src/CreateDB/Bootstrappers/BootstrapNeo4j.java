package CreateDB.Bootstrappers;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

public class BootstrapNeo4j {
    private static final Label endpointLabel = DynamicLabel.label("Endpoint");
    private static final RelationshipType connectedRelationship = DynamicRelationshipType.withName("CONNECTED");
    private static final String nodeKey = "Id";
    private static final String regexPattern = "(?<fromNodeId>[0-9]+)[\\s]+(?<toNodeId>[0-9]+)";
    private static final String databaseName = "neo4j-db";

    public static RelationshipType getConnectedRelationship() { return connectedRelationship; }

    public static void bootstrap(String sourceFileName) throws IOException {
        deleteFileOrDirectory(new File(getFileName()));
        BatchInserter inserter = null;

        System.out.printf("Preparing to create Neo4j graph database. ");
        long start = System.nanoTime();
        try (FileReader fileReader = new FileReader(new File(sourceFileName))
        ) {
            try (BufferedReader br = new BufferedReader(fileReader)) {
                inserter = BatchInserters.inserter(getFileName());
                loadSourceFile(br, inserter);
            }
        }
        catch (FileNotFoundException e) { e.printStackTrace(); }

        if (inserter != null) { inserter.shutdown(); }
        long elapsedTime = System.nanoTime() - start;
        System.out.printf("Completed in %.3f seconds.\n", (float) elapsedTime / Math.pow(10,9));
    }

    private static void loadSourceFile(BufferedReader br, BatchInserter inserter) throws IOException {
        inserter.createDeferredSchemaIndex(endpointLabel).on(nodeKey).create();

        String line;
        Pattern pattern = Pattern.compile(regexPattern);
        Matcher matcher;

        //line = br.readLine();
        //matcher = pattern.matcher(line);
        //processLine(matcher, inserter);
        Map<String, Object> newNode = new HashMap<>();
        Map<Integer, Long> existingNodes = new HashMap<>();
        while ((line = br.readLine()) != null) {
            matcher = pattern.matcher(line);
            processLine(matcher, inserter, existingNodes, newNode);
        }
    }

    private static void processLine(Matcher matcher, BatchInserter inserter, Map<Integer, Long> existingNodes,
                                    Map<String, Object> newNode) {
        long graphFromId;
        long graphToId;
        Integer sourceFromId;
        Integer sourceToId;
        if (matcher.find()) {
            sourceFromId = Integer.parseInt(matcher.group("fromNodeId"));
            sourceToId = Integer.parseInt(matcher.group("toNodeId"));
            if (existingNodes.containsKey(sourceFromId)) {
                graphFromId = existingNodes.get(sourceFromId);
            } else {
                newNode.put(nodeKey, sourceFromId);
                graphFromId = inserter.createNode(newNode, endpointLabel);
                existingNodes.put(sourceFromId, graphFromId);
            }
            if (existingNodes.containsKey(sourceToId)) {
                graphToId = existingNodes.get(sourceToId);
            } else {
                newNode.put(nodeKey, sourceToId);
                graphToId = inserter.createNode(newNode, endpointLabel);
                existingNodes.put(sourceToId, graphToId);
            }
            inserter.createRelationship(graphFromId, graphToId, connectedRelationship, null);
        }
    }

    private static String getFileName() {
        Path path = Paths.get(new File("").getAbsolutePath(), databaseName);
        path = path.normalize();
        return path.toString();
    }

    private static void deleteFileOrDirectory(File file)
    {
        if (file.exists())
        {
            if ( file.isDirectory() )
            {
                for ( File child : file.listFiles() )
                {
                    deleteFileOrDirectory(child);
                }
            }
            file.delete();
        }
    }

    public static GraphDatabaseService getDatabaseService() {
        return new GraphDatabaseFactory().newEmbeddedDatabase(getFileName());
    }
}
