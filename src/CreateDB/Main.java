package CreateDB;

import Controllers.GraphController;
import Controllers.MongoTrajectoryController;
import Controllers.RelationalGraphController;
import Controllers.RelationalTrajectoryController;
import com.mongodb.MongoException;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;

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
            rgc.loadGraphDatabase(graphSourceFileName);
            gc.finish();
            rgc.finish();
        } catch (IOException e) { e.printStackTrace(); System.exit(1); }
    }

    private static void createTrajectory() {
        try {
            createRelationalTrajectories();
            createMongoTrajectories();
        } catch (IOException e) { e.printStackTrace(); System.exit(1); }
    }

    private static void createRelationalTrajectories() throws IOException {
        RelationalTrajectoryController rtc = new RelationalTrajectoryController();
        rtc.loadTrajectoryDatabase(trajectorySourceDir);
        rtc.finish();
    }

    private static void createMongoTrajectories() throws IOException {
        try {
            MongoTrajectoryController mtc = new MongoTrajectoryController();
            mtc.loadGraphDatabase(trajectorySourceDir);
        }
        catch (UnknownHostException e) { e.printStackTrace(); }
        catch (MongoException e) { e.printStackTrace(); }

    }
}
