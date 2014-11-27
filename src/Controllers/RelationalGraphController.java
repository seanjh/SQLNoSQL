package Controllers;

import CreateDB.Bootstrappers.BootstrapMySQL;
import DatabaseHelper.QueryHelper;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class RelationalGraphController extends RelationalController {

    // Query files
    //static final String allNodesFileName = "Graph/AllNodes.sql";
    static final String nodeExistsFileName = "Graph/NodeExists.sql";
    static final String neighborCountFileName = "Graph/NeighbourCount.sql";
    static final String reachabilityCountFileName = "Graph/ReachabilityCount.sql";

    public RelationalGraphController() {
        super();
    }

    public void loadGraphDatabase(String sourcePath) {
        try {
            BootstrapMySQL.bootstrapGraphDB(getRelationalConnection(), sourcePath);
        } catch (SQLException e) { e.printStackTrace(); }
    }

    public static boolean graphNodeExists(Connection conn, int nodeId) throws SQLException, IOException {
        // Returns true if a result was returned, and false otherwise
        String statementStr = QueryHelper.getQueryStatementFromFile(nodeExistsFileName);

        boolean result;
        try (PreparedStatement stmt = conn.prepareStatement(statementStr)) {
            stmt.setInt(1, nodeId);
            try (ResultSet rs = stmt.executeQuery()) {
                result = rs.next();
                conn.commit();
                return result;
            }
        }
    }

    public static Iterator<Map<String, Object>> getNeighborCount(Connection conn, int queryParameter)
            throws SQLException, IOException {
        String statementStr = QueryHelper.getQueryStatementFromFile(neighborCountFileName);
        List<Map<String, Object>> results = new LinkedList<>();
        Map<String, Object> oneResult;

        ResultSetMetaData resultMeta;
        // Try to load the connection
        try (PreparedStatement stmt = conn.prepareStatement(statementStr)) {
            stmt.setInt(1, queryParameter);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    oneResult = new HashMap<>();
                    //resultMeta = rs.getMetaData();
                    oneResult.put("FromNodeId", rs.getInt("FromNodeId"));
                    oneResult.put("NeighbourCount", rs.getInt("NeighbourCount"));
                    results.add(oneResult);
                }
            }
        }
        conn.commit();

        return results.iterator();
    }

    public static Map<String, Object> getReachabilityCount(Connection conn, int queryParameter)
            throws SQLException, IOException {
        String statementRoot = QueryHelper.getQueryStatementFromFile(reachabilityCountFileName);

        LinkedList<Integer> nodesToQuery = new LinkedList<>();
        nodesToQuery.add(queryParameter);

        Set<Integer> reachableSet = new HashSet<>();
        Set<Integer> queriedSet = new HashSet<>();
        int node;
        int queryCount = 0;
        long start = System.nanoTime();
        String statementStr;
        while (!nodesToQuery.isEmpty()) {
            statementStr = QueryHelper.completeStatement(statementRoot, nodesToQuery.size());
            try (PreparedStatement stmt = conn.prepareStatement(statementStr)) {
                QueryHelper.setParameterList(stmt, nodesToQuery.descendingIterator(),
                        nodesToQuery.size());
                // Add each node from the toQuery stack to the queried set
                while (!nodesToQuery.isEmpty()) {
                    queriedSet.add(nodesToQuery.pop());
                }
                try (ResultSet rs = stmt.executeQuery()) {
                    queryCount++;
                    while (rs.next()) {
                        node = rs.getInt("ToNodeID");
                        reachableSet.add(node);
                        // Queue this node to be queried, if it has not been queried already
                        if (!queriedSet.contains(node)) {
                            nodesToQuery.push(node);
                        }
                    }

                }
            }
        }
        conn.commit();

        Map<String, Object> result = new HashMap<>();
        result.put("ReachabilityCount", reachableSet.size());
        return result;
    }
}
