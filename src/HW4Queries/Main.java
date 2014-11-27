package HW4Queries;

import Controllers.*;
import org.neo4j.graphdb.GraphDatabaseService;

import java.io.IOException;
import java.sql.*;
import java.sql.Connection;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;
import java.util.Date;

public class Main {
    static final int maxNodeId = 1971278;
    static final String graphDatabaseName = "GraphNetworks";
    static final String trajectoriesDatabaseName = "GeoLifeTrajectories";
    static final String minTrajDateStr = "2000-01-01 00:00:00";
    static final String maxTrajDateStr = "2012-07-27 00:00:00";

    public static void main(String[] args) {
        try {
            GraphController gc = new GraphController();
            RelationalGraphController rgc = new RelationalGraphController();
            rgc.setRelationalDatabase(graphDatabaseName);
            executeGraphQueries(rgc.getRelationalConnection(), gc.getGraphDatabaseService());
            gc.finish();
            rgc.finish();

            RelationalTrajectoryController rtc = new RelationalTrajectoryController();
            rtc.setRelationalDatabase(trajectoriesDatabaseName);
            MongoTrajectoryController mtc = new MongoTrajectoryController();
            executeTrajectoryQueries(rtc, mtc);
            rtc.finish();
            mtc.finish();
        }
        catch (SQLException e) { e.printStackTrace(); }
        catch (IOException e) { e.printStackTrace(); }
    }

    private static void executeGraphQueries(Connection conn, GraphDatabaseService graphDb)
            throws SQLException, IOException {
        final int queryNum = 2;
        int[] queryValues = new int[queryNum];
        for (int i=0; i<queryNum; i++) {
            queryValues[i] = randInt(0, maxNodeId);
        }

        List<Long> sqlNC = new ArrayList<>(queryNum);
        List<Long> graphNC = new ArrayList<>(queryNum);
        List<Long> sqlR = new ArrayList<>(queryNum);
        List<Long> graphR = new ArrayList<>(queryNum);
        long start;
        long elapsed;
        for (int value : queryValues) {
            start = ResultController.printStart("MySQL Node Neighbor Count", "Node", Integer.toString(value));
            ResultController.printResult(RelationalGraphController.getNeighborCount(conn, value));
            elapsed = ResultController.printEnd(start);
            sqlNC.add(elapsed);

            start = ResultController.printStart("Neo4j Node Neighbor Count", "Node", Integer.toString(value));
            ResultController.printResult(GraphController.executeNeighborCountCypher(graphDb, value));
            elapsed = ResultController.printEnd(start);
            graphNC.add(elapsed);

            start = ResultController.printStart("MySQL Reachability Count", "Node", Integer.toString(value));
            ResultController.printResult(RelationalGraphController.getReachabilityCount(conn, value));
            elapsed = ResultController.printEnd(start);
            sqlR.add(elapsed);

            start = ResultController.printStart("Neo4j Reachability Count", "Node", Integer.toString(value));
            ResultController.printResult(GraphController.reachabilityTraversal(graphDb, value));
            elapsed = ResultController.printEnd(start);
            graphR.add(elapsed);

            //getTrajectorySetCount()
        }

        /*
        For the each query, compare the execution on MySQL to the execution on the NoSQL systems.
        Report the minimal time, maximal time and average time on the runs you conducted.
         */
        ResultController.printElapsedReview(sqlNC, "MySQL Node Neighbor Count");
        ResultController.printElapsedReview(graphNC, "Neo4j Node Neighbor Count");
        ResultController.printElapsedReview(sqlR, "MySQL Reachability Count");
        ResultController.printElapsedReview(graphR, "Neo4j Reachability Count");
    }

    private static void executeTrajectoryQueries(RelationalTrajectoryController rtc, MongoTrajectoryController mtc)
            throws SQLException {
        final int queryNum = 2;
        final String fileNameSample = "20081023175854.plt";
        final int userId = 3;

        long start;

        start = ResultController.printStart("MySQL Trajectory Set Count", "Trajectory", fileNameSample);
        ResultController.printResult(RelationalTrajectoryController.getTrajectorySetCount(rtc.getRelationalConnection(), fileNameSample, userId));
        ResultController.printEnd(start);

        start = ResultController.printStart("MongoDB Trajectory Set Count", "Trajectory", fileNameSample);
        ResultController.printResult(MongoTrajectoryController.getTrajectorySetCount(mtc.getCollection(), fileNameSample, userId));
        ResultController.printEnd(start);

        List<Long> sqlMD = new ArrayList<>(queryNum);
        List<Long> mongoMD = new ArrayList<>(queryNum);
        long elapsed;
        for (int i=0; i<queryNum; i++) {
            LocalDate tmpDate = randomDateBetweenDates(minTrajDateStr, maxTrajDateStr);

            start = ResultController.printStart("MySQL Trajectory Measures on Date", "Date", tmpDate.toString());
            ResultController.printResult(RelationalTrajectoryController.getMeasuresOnDateCount(rtc.getRelationalConnection(), utilDateFromLocalDate(tmpDate)));
            elapsed = ResultController.printEnd(start);
            sqlMD.add(elapsed);

            start = ResultController.printStart("MongoDB Trajectory Measures on Date", "Date", tmpDate.toString());
            ResultController.printResult(MongoTrajectoryController.getMeasuresOnDateCount(mtc.getCollection(), utilDateFromLocalDate(tmpDate)));
            elapsed = ResultController.printEnd(start);
            mongoMD.add(elapsed);
        }

        ResultController.printElapsedReview(sqlMD, "MySQL Trajectory Measures on Date");
        ResultController.printElapsedReview(mongoMD, "MongoDB Trajectory Measures on Date");
    }

    // via Greg Case @ http://stackoverflow.com/a/363692
    public static int randInt(int min, int max) {
        Random rand = new Random();
        return rand.nextInt((max - min) + 1) + min;
    }

    // via http://stackoverflow.com/a/11016560
    public static LocalDate randomDateBetweenDates(String afterDateTime, String beforeDateTime) {
        long offset = Timestamp.valueOf(afterDateTime).getTime();
        long end = Timestamp.valueOf(beforeDateTime).getTime();
        long diff = end - offset + 1;
        Timestamp timestamp =  new Timestamp(offset + (long)(Math.random() * diff));
        return timestamp.toLocalDateTime().toLocalDate();
        //return Date.from(timestamp.toInstant());
    }

    private static Date utilDateFromLocalDate(LocalDate ld) {
        Instant instant = ld.atStartOfDay().atZone(ZoneId.of("GMT")).toInstant();
        return Date.from(instant);
    }
}
