package CreateDB.Bootstrappers;

import DatabaseHelper.QueryHelper;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class BootstrapMySQL {
    private static final String createGraphFileName = "Graph/RoadNetworkDatabase.sql";
    private static final String loadGraphFileName = "Graph/LoadGraphDataInFile.sql";
    private static final String insertGraphNodesFilesName = "Graph/InsertUniqueNodes.sql";
    private static final String insertTrajectoryUserFileName = "Trajectory/InsertTrajectoryUser.sql";
    private static final String insertTrajectorySetFileName = "Trajectory/InsertTrajectorySet.sql";
    private static final String createTrajectoryFileName = "Trajectory/TrajectoryDatabase.sql";
    private static final String loadTrajectoryMeasureInFile = "Trajectory/LoadTrajectoryMeasuresInFile.sql";
    private static final String lastAutoIncrementValueQuery = "SELECT LAST_INSERT_ID();";
    //private static final String insertPairsFileName = "InsertNodePairs.sql";

    public static void bootstrapTrajectories(Connection conn, String source) throws SQLException, IOException {
        // Create the trajectory database and tables
        long start = System.nanoTime();
        createTrajectoryDB(conn);

        // Source directory should include a list of numbered directories, 1 for each user (e.g. 000, 001)
        File sourceDir = Paths.get(source).toFile();
        if (!sourceDir.isDirectory())
            throw new IOException(source + " is not a directory");
        int setId = 1;
        int userId;
        for (File userDir : sourceDir.listFiles()) {
            // Insert the user into the database, and get that user's Id from the database
            insertTrajectoryUser(conn, Integer.parseInt(userDir.getName()));

            // Each numbered user directory should include a /Trajectory directory
            File plotsDir = Paths.get(userDir.toString(), "Trajectory").toFile();
            if (!plotsDir.isDirectory()) {
                conn.rollback();
                throw new IOException(source + " is not a directory");
            }
            //conn.commit();

            // Each /[userno]/Trajectory directory should include 1 or more plt files. These plot files
            // contain 1 or more the trajectory measures
            userId = Integer.parseInt(userDir.getName());
            for (File plotFile : plotsDir.listFiles()) {
                try {
                    if (plotFile.isFile()) {
                        // Each plot file represents 1 trajectory set
                        // Insert the trajectory set for this user in the database
                        insertTrajectorySet(conn, userId, setId, plotFile.getName());
                        insertTrajectoryMeasures(conn, plotFile.toString(), setId);
                        //System.out.printf("Finished loading %s\n", plotFile.toString());
                        setId++;
                        conn.commit();
                    } else {
                        throw new IOException(plotFile + " must be a valid .plt file.");
                    }
                } catch (SQLException e) {
                    conn.rollback();
                    throw e;
                }
            }
            userId++;
        }
        long elapsedTime = System.nanoTime() - start;
        System.out.printf("Completed in %.3f seconds.\n", (float) elapsedTime / Math.pow(10,9));
    }

    private static void createTrajectoryDB(Connection conn) throws SQLException {
        createDB(conn, createTrajectoryFileName);
    }

    /*private static int getLastID(Connection conn) throws SQLException {
        int lastId;
        try (PreparedStatement stmt = conn.prepareStatement(lastAutoIncrementValueQuery)) {
            try (ResultSet result = stmt.executeQuery()) {
                lastId = result.getInt(1);
                stmt.execute();
            }
        }
        return lastId;
    }*/

    private static void insertTrajectoryUser(Connection conn, int userId) throws SQLException {
        String statementStr = "";
        try {
            statementStr = QueryHelper.getQueryStatementFromFile(insertTrajectoryUserFileName);
        }
        catch (IOException e) { e.printStackTrace(); System.exit(1); }

        try (PreparedStatement stmt = conn.prepareStatement(statementStr)) {
            stmt.setInt(1, userId);
            stmt.execute();
        }
    }

    public static String getSQLDateTimeStringFromDateTimeFileName(String filename) {
        DateTimeFormatter inFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        LocalDateTime date = LocalDateTime.parse(filename.substring(0,14), inFormatter);
        DateTimeFormatter outFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return date.format(outFormatter);
    }

    private static void insertTrajectorySet(Connection conn, int userId, int setId, String fileName) throws SQLException {
        String statementStr = "";
        try {
            statementStr = QueryHelper.getQueryStatementFromFile(insertTrajectorySetFileName);
        }
        catch (IOException e) { e.printStackTrace(); System.exit(1); }

        try (PreparedStatement stmt = conn.prepareStatement(statementStr)) {
            stmt.setInt(1, setId);
            stmt.setString(2, getSQLDateTimeStringFromDateTimeFileName(fileName));
            stmt.setInt(3, userId);
            stmt.execute();
        }
    }

    private static void insertTrajectoryMeasures(Connection conn, String fileName, int setId) throws SQLException {
        String statementStr = "";
        try {
            statementStr = QueryHelper.getQueryStatementFromFile(loadTrajectoryMeasureInFile);
        }
        catch (IOException e) { e.printStackTrace(); System.exit(1); }

        try (PreparedStatement stmt = conn.prepareStatement(statementStr)) {
            //stmt.setInt(1, setId);
            //stmt.setString(2, fileName);
            stmt.setString(1, fileName);
            stmt.setInt(2, setId);
            stmt.execute();
        }
    }

    public static void bootstrapGraphDB(Connection conn, String sourceFileName) throws SQLException {
        System.out.printf("Preparing to load graph data to MySQL database.\n");
        long start = System.nanoTime();
        createGraphDB(conn);
        loadGraphData(conn, sourceFileName);
        long elapsedTime = System.nanoTime() - start;
        System.out.printf("Completed in %.3f seconds.\n", (float) elapsedTime / Math.pow(10,9));
    }

    private static void createGraphDB(Connection conn) throws SQLException {
        createDB(conn, createGraphFileName);
    }

    private static void loadGraphData(Connection conn, String sourceFileName) throws SQLException {
        long start = System.nanoTime();
        bulkInsertGraph(conn, sourceFileName);
        long elapsedTime = System.nanoTime() - start;
        System.out.printf("Completed in %.3f seconds.\n", (float) elapsedTime / Math.pow(10,9));

        start = System.nanoTime();
        insertUniqueGraphNodes(conn);
        elapsedTime = System.nanoTime() - start;
        System.out.printf("Completed in %.3f seconds.\n", (float) elapsedTime / Math.pow(10,9));
    }

    private static void bulkInsertGraph(Connection conn, String sourceFileName) throws SQLException {
        String loadStatement = "";
        try {
            loadStatement = QueryHelper.getQueryStatementFromFile(loadGraphFileName);
        }
        catch (IOException e) { e.printStackTrace(); System.exit(1); }

        try (PreparedStatement stmt = conn.prepareStatement(loadStatement)) {
            stmt.setString(1, sourceFileName);
            System.out.printf("\tPreparing to execute load data statement. ");
            stmt.execute();
            //System.out.printf("Completed.\n");
        } catch (Exception e) { conn.rollback(); }
        conn.commit();
    }

    private static void insertUniqueGraphNodes(Connection conn) throws SQLException {
        String insertStatement = "";
        try {
            insertStatement = QueryHelper.getQueryStatementFromFile(insertGraphNodesFilesName);
        }
        catch (IOException e) { e.printStackTrace(); System.exit(1); }

        try ( PreparedStatement stmt = conn.prepareStatement(insertStatement) ) {
            System.out.printf("\tPreparing to populate unique nodes table. ");
            stmt.execute();
            conn.commit();
        }
    }

    private static void createDB(Connection conn, String filename) throws SQLException {
        String createStatement = "";
        try {
            createStatement = QueryHelper.getQueryStatementFromFile(filename);
        }
        catch (IOException e) { e.printStackTrace(); System.exit(1); }

        // Create the database and tables if it doesn't exist already
        System.out.printf("\tPreparing to execute create statement. ");
        long start = System.nanoTime();
        for (String sqlString : createStatement.split(";")) {
            if (sqlString.trim().isEmpty()) {
                continue;
            }
            PreparedStatement stmt = conn.prepareStatement(sqlString);
            stmt.execute();
        }
        conn.commit();
        long elapsedTime = System.nanoTime() - start;
        System.out.printf("Completed in %.3f seconds.\n", (float) elapsedTime / Math.pow(10,9));
    }
}
