package CreateDB.Bootstrappers;

import Controllers.MongoTrajectoryController;
import com.mongodb.*;

import java.io.*;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class BootStrapMongoDB {
    private static final String databaseName = "GeoLifeTrajectories";
    private static final String collectionName = "Trajectories";
    private static final String regexPattern = "(?<latitude>[0-9\\.]+){1}(?<sep>[\\s]*,[\\s]*){1}" +
            "(?<longitude>[0-9\\.]+){1}\\k<sep>{1}(?<zeroField>[0]){1}\\k<sep>{1}(?<altitude>[0-9]+\\.*[0-9]*){1}" +
            "\\k<sep>{1}(?<dateOffset>[0-9\\\\.]+){1}\\k<sep>{1}(?<year>[0-9]{4})-(?<month>[0-9]{2})-(?<day>[0-9]{2})" +
            "\\k<sep>{1}(?<hour>[0-9]{2}):(?<minute>[0-9]{2}):(?<second>[0-9]{2})";

    public static void bootstrap(MongoClient mongo, String sourceDirStr) throws IOException, MongoException {
        long start = System.nanoTime();
        DBCollection coll = createTrajectoryDatabase(mongo);
        BulkWriteOperation builder;
        BulkWriteResult result;

        File sourceDir = Paths.get(sourceDirStr).toFile();
        if (!sourceDir.isDirectory())
            throw new IOException(sourceDirStr + " is not a directory");

        int userId;
        BasicDBObject oneSet = null;
        ArrayList<BasicDBObject> measures = null;
        //int i = 0;
        for (File userDir : sourceDir.listFiles()) {
            try {
                userId = Integer.parseInt(userDir.getName());
            } catch (NumberFormatException e) { continue; }

            File plotsDir = Paths.get(userDir.toString(), "Trajectory").toFile();
            if (!plotsDir.isDirectory()) {
                throw new IOException(sourceDirStr + " is not a directory");
            }

            for (File plotFile : plotsDir.listFiles()) {
                builder = coll.initializeUnorderedBulkOperation();
                if (plotFile.isFile()) {
                    oneSet = MongoTrajectoryController.makeSetDBObject(plotFile.getName(), userId);
                    int linesGuess = (int) plotFile.length() / 63;
                    measures = new ArrayList<>(linesGuess);
                    processTrajectoryMeasures(plotFile, measures);
                    oneSet.put("measures", measures);
                }
                // Insert the trajectory set, containing many measures
                builder.insert(oneSet);
                result = builder.execute();
                if (result.getInsertedCount() < 1) {
                    throw new MongoException("Failed to insert trajectory set");
                }
                //i++;
                //System.out.printf("#%d:\tInserted %d measures for %d from %s\n", i, measures.size(), userId, plotFile);
            }
        }
        long elapsedTime = System.nanoTime() - start;
        System.out.printf("Completed in %.3f seconds.\n", (float) elapsedTime / Math.pow(10,9));
    }

    private static void processTrajectoryMeasures(File plotFile, ArrayList<BasicDBObject> measures) throws IOException {
        BufferedReader in = new BufferedReader(new FileReader(plotFile));

        Pattern pattern = Pattern.compile(regexPattern);
        Matcher matcher;
        String line = "";
        while ((line = in.readLine()) != null) {
            matcher = pattern.matcher(line);
            if (matcher.find()) {
                measures.add(MongoTrajectoryController.makeMeasureDBObject(
                        matcher.group("latitude"), matcher.group("longitude"),
                        matcher.group("zeroField"), matcher.group("altitude"),
                        matcher.group("dateOffset"), matcher.group("year"),
                        matcher.group("month"), matcher.group("day"),
                        matcher.group("hour"), matcher.group("minute"),
                        matcher.group("second")
                        ));
            }
        }
    }

    private static DBCollection createTrajectoryDatabase(MongoClient mongo) {
        mongo.dropDatabase(databaseName);
        DB db = mongo.getDB(databaseName);
        return db.getCollection(collectionName);
    }

    public static String getDatabaseName() { return databaseName; }
    public static String getCollectionName() { return collectionName; }
}
