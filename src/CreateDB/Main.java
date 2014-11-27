package CreateDB;

import Controllers.*;
import com.mongodb.MongoException;

import java.io.IOException;
import java.net.UnknownHostException;

public class Main {
    private static final String graphSourceFileName = "/Users/sean/Sync/cornell/databases/data/roadNet-CA.txt";
    private static final String trajectorySourceDir = "/Users/sean/Sync/cornell/databases/data/geolife/Data";

    public static void main(String[] args) {
        createGraphs();
        createTrajectory();
    }

    private static void createGraphs() {
        try {
            GraphController gc = new GraphController();
            RelationalGraphController rgc = new RelationalGraphController();
            gc.loadGraphDB(graphSourceFileName);
            rgc.createGraphDatabase(graphSourceFileName);
            gc.finish();
            rgc.finish();
        } catch (IOException e) { e.printStackTrace(); System.exit(1); }
    }

    private static void createTrajectory() {
        try {
            createRelationalTrajectories();
            createMongoTrajectories();
            createRedisTrajectories();
        } catch (IOException e) { e.printStackTrace(); System.exit(1); }
    }

    private static void createRelationalTrajectories() throws IOException {
        RelationalTrajectoryController rtc = new RelationalTrajectoryController();
        rtc.createTrajectoryDatabase(trajectorySourceDir);
        rtc.finish();
    }

    private static void createMongoTrajectories() throws IOException {
        try {
            MongoTrajectoryController mtc = new MongoTrajectoryController();
            mtc.createDatabase(trajectorySourceDir);
            mtc.finish();
        }
        catch (UnknownHostException e) { e.printStackTrace(); }
        catch (MongoException e) { e.printStackTrace(); }
    }

    private static void createRedisTrajectories() throws IOException {
        RedisTrajectoryController dtc = new RedisTrajectoryController();
        dtc.createDatabase(trajectorySourceDir);
        dtc.finish();
    }
}
