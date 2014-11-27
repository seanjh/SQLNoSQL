package CreateDB.Bootstrappers;
;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BootstrapRedis {
    private static final int databaseIndex = 11;
    private static final String trajSetKeyBase = "trajset";
    private static final String measureKeyBase = "measure";
    private static final String userKeyBase = "user";
    private static final String dateKeyBase = "date";
    private static final String regexPattern = "(?<latitude>[0-9\\.]+){1}(?<sep>[\\s]*,[\\s]*){1}" +
            "(?<longitude>[0-9\\.]+){1}\\k<sep>{1}(?<zeroField>[0]){1}\\k<sep>{1}(?<altitude>[0-9]+\\.*[0-9]*){1}" +
            "\\k<sep>{1}(?<dateOffset>[0-9\\\\.]+){1}\\k<sep>{1}(?<year>[0-9]{4})-(?<month>[0-9]{2})-(?<day>[0-9]{2})" +
            "\\k<sep>{1}(?<hour>[0-9]{2}):(?<minute>[0-9]{2}):(?<second>[0-9]{2})";
    private static final Pattern pattern = Pattern.compile(regexPattern);

    public static void bootstrap(Jedis jedis, String sourceDirStr) throws IOException {
        System.out.printf("Preparing to load trajectory data to Redis. ");
        // Delete the existing database, if it exists
        jedis.select(databaseIndex);
        jedis.flushDB();
        //jedis.flushAll();


        long start = System.nanoTime();
        File sourceDir = Paths.get(sourceDirStr).toFile();
        if (!sourceDir.isDirectory())
            throw new IOException(sourceDirStr + " is not a directory");

        RedisKeyIds ids = new RedisKeyIds();
        int userId;
        for (File userDir : sourceDir.listFiles()) {
            try {
                userId = Integer.parseInt(userDir.getName());
            } catch (NumberFormatException e) { continue; }

            File plotsDir = Paths.get(userDir.toString(), "Trajectory").toFile();
            if (!plotsDir.isDirectory()) {
                throw new IOException(sourceDirStr + " is not a directory");
            }
            for (File plotFile : plotsDir.listFiles()) {
                if (plotFile.isFile()) {
                    processTrajectoryMeasures(jedis, plotFile, ids);
                    //System.out.printf("executing lpush to %s of %s\n", getUserKey(userId), getTrajSetKey(plotFile.getName()));
                    jedis.lpush(getUserKey(userId), getTrajSetKey(plotFile.getName()));
                }
            }
        }
        long elapsedTime = System.nanoTime() - start;
        System.out.printf("Completed in %.3f seconds.\n", (float) elapsedTime / Math.pow(10,9));
    }

    private static String getTrajSetKey(String fileName) {
        return trajSetKeyBase + ":" + fileName.substring(0, 14);
    }

    private static String getMeasureKey(int measureId) {
        return measureKeyBase + ":" + measureId;
    }

    private static String getUserKey(int userId) {
        return userKeyBase + ":" + userId;
    }

    private static String getDateKey(String year, String month, String day) {
        return dateKeyBase + ":" + year + month + day;
    }

    private static void processTrajectoryMeasures(Jedis jedis, File plotFile, RedisKeyIds ids) throws IOException {
        BufferedReader in = new BufferedReader(new FileReader(plotFile));

        Matcher matcher;
        String line;
        Map<String, String> fields;
        String trajSetKey = getTrajSetKey(plotFile.getName());
        String measureKey;
        String year, month, day;
        while ((line = in.readLine()) != null) {
            matcher = pattern.matcher(line);
            if (matcher.find()) {
                fields = new HashMap<>();
                fields.put("latitude", matcher.group("latitude"));
                fields.put("longitude", matcher.group("longitude"));
                fields.put("zeroField", matcher.group("zeroField"));
                fields.put("altitude", matcher.group("altitude"));
                fields.put("dateOffset", matcher.group("dateOffset"));
                year = matcher.group("year");
                month = matcher.group("month");
                day = matcher.group("day");
                fields.put("dateTime",
                            year + "-" + month + "-" +
                            day + " " + matcher.group("hour") + ":" +
                            matcher.group("minute") + ":" + matcher.group("second")
                        );
                measureKey = getMeasureKey(ids.getNextMeasureId());
                // Make this measure a hash
                jedis.hmset(measureKey, fields);
                //System.out.printf("\texecuting lpush to %s of %s\n", trajSetKey, measureKey);
                // Add this measure key to the trajectory set list
                jedis.lpush(trajSetKey, measureKey);
                // Also add this measure the the appropriate date list
                jedis.lpush(getDateKey(year, month, day), measureKey);
            }
        }
    }
}

class RedisKeyIds {
    public int measure = 0;

    public RedisKeyIds() {}

    public int getNextMeasureId() { return ++measure; }
    public int getLastMeasureId() { return measure; }
}
