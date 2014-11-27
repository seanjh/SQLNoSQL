package Controllers;

import CreateDB.Bootstrappers.BootStrapMongoDB;
import com.mongodb.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class MongoTrajectoryController {
    private MongoClient mongo;
    private DB db;
    private DBCollection collection;

    public MongoTrajectoryController() throws UnknownHostException {
        this.mongo = new MongoClient();
        this.db = mongo.getDB(BootStrapMongoDB.getDatabaseName());
        this.collection = this.db.getCollection(BootStrapMongoDB.getCollectionName());
    }

    public DB getDB() { return this.db; }
    public DBCollection getCollection() { return this.collection; }

    public void loadGraphDatabase(String sourcePath) throws IOException, MongoException {
        BootStrapMongoDB.bootstrap(this.mongo, sourcePath);
    }

    public MongoClient getMongoClient() {
        return this.mongo;
    }

    /*public static BasicDBObject makeMeasureDBObject(String line) {
        String[] elements = line.split(",");
        BasicDBObject dbObject = new BasicDBObject();
        dbObject.put("latitude", Float.parseFloat(elements[0]));
        dbObject.put("longitude", Float.parseFloat(elements[1]));
        dbObject.put("zero", Byte.parseByte(elements[2]));
        dbObject.put("altitude", Float.parseFloat(elements[3]));
        dbObject.put("days", Float.parseFloat(elements[4]));
        DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime dateTime = LocalDateTime.parse(elements[4] + " " + elements[5], dateTimeFormatter);
        dbObject.put("dateTimeGMT", dateTime);
        return dbObject;
    }*/

    public static BasicDBObject makeMeasureDBObject(String latitude, String longitude, String zero,
                                                    String altitude, String daysSince, String year, String month,
                                                    String day, String hour, String minute, String second) {
        BasicDBObject dbObject = new BasicDBObject();
        dbObject.put("latitude", Float.parseFloat(latitude));
        dbObject.put("longitude", Float.parseFloat(longitude));
        dbObject.put("zero", Byte.parseByte(zero));
        dbObject.put("altitude", Float.parseFloat(altitude));
        dbObject.put("days", Float.parseFloat(daysSince));
        LocalDateTime dateTime = LocalDateTime.of(Integer.parseInt(year), Integer.parseInt(month), Integer.parseInt(day),
                Integer.parseInt(hour), Integer.parseInt(minute), Integer.parseInt(second));
        // code here from http://blog.progs.be/542/date-to-java-time

        dbObject.put("dateGMT", dateFromGMTLocalDateTime(dateTime, "GMT"));
        return dbObject;
    }

    private static Date dateFromGMTLocalDateTime(LocalDateTime dateTime, String zone) {
        Instant instant = dateTime.atZone(ZoneId.of(zone)).toInstant();
        return Date.from(instant);
    }


    public static BasicDBObject makeSetDBObject(String dateTimeFileName, int userId) {
        DateTimeFormatter inFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        LocalDateTime dateTime = LocalDateTime.parse(dateTimeFileName.substring(0,14), inFormatter);
        BasicDBObject setObject = new BasicDBObject("startTime", dateFromGMTLocalDateTime(dateTime, "GMT"));
        setObject.put("userId", userId);
        return setObject;
    }

    public static Iterator<Map<String, Object>> getTrajectorySetCount(DBCollection coll, String dateTimeFileName, int userId) {
        AggregationOutput output = coll.aggregate(makeTrajectorySetCountAggregateList(dateTimeFileName, userId));
        Iterator<DBObject> resultList = output.results().iterator();

        LinkedList<Map<String, Object>> result = new LinkedList<>();
        while (resultList.hasNext()) {
            result.add(resultList.next().toMap());
        }

        return result.iterator();
    }

    public static Iterator<Map<String, Object>> getMeasuresOnDateCount(DBCollection coll, Date date) {
        Instant instant = Instant.ofEpochMilli(date.getTime());
        LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        AggregationOutput output = coll.aggregate(makeMeasuresOnDateAggregateList(ldt));

        Iterator<DBObject> resultList = output.results().iterator();

        LinkedList<Map<String, Object>> result = new LinkedList<>();
        while (resultList.hasNext()) {
            result.add(resultList.next().toMap());
        }

        return result.iterator();
    }

    private static List<DBObject> makeTrajectorySetCountAggregateList(String dateTimeFileName, int userId) {
        DBObject tmpObject;

        List<DBObject> objectList = new ArrayList<>(2);
        // Make the $match object. This includes an $and object with the filter parameters
        DateTimeFormatter inFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        LocalDateTime dateTime = LocalDateTime.parse(dateTimeFileName.substring(0, 14), inFormatter);
        //tmpObject.put("startTime", dateFromGMTLocalDateTime(dateTime, "GMT"));
        tmpObject = new BasicDBObject("startTime", new BasicDBObject("$gte", dateFromGMTLocalDateTime(dateTime, "GMT")));
        objectList.add(tmpObject);
        tmpObject = new BasicDBObject("startTime", new BasicDBObject("$lte", dateFromGMTLocalDateTime(dateTime, "GMT")));
        objectList.add(tmpObject);
        tmpObject = new BasicDBObject("userId", userId);
        objectList.add(tmpObject);
        tmpObject = new BasicDBObject("$and", objectList);
        BasicDBObject matchObject = new BasicDBObject("$match", tmpObject);

        // Make the $project object
        tmpObject = new BasicDBObject();
        tmpObject.put("_id", 0);
        tmpObject.put("startTime", 1);
        //tmpObject.put("userId", 1);
        tmpObject.put("measureCount", new BasicDBObject("$size", "$measures"));
        BasicDBObject projectObject = new BasicDBObject("$project", tmpObject);

        // Add the $project and $match objects to an array
        objectList = new ArrayList<>(2);
        objectList.add(matchObject);
        objectList.add(projectObject);

        //System.out.println(objectList.toString());

        return objectList;
    }

    private static List<DBObject> makeMeasuresOnDateAggregateList(LocalDateTime dateTime) {
        DBObject queryObject = new BasicDBObject();
        List<DBObject> aggregateList = new ArrayList<>(3);
        DBObject tmpObject;

        // Make the $unwind object
        tmpObject = new BasicDBObject("$unwind", "$measures");
        aggregateList.add(tmpObject);

        // Make the $match object
        List<DBObject> andList = new ArrayList<>(2);
        tmpObject = new BasicDBObject("measures.dateGMT", new BasicDBObject("$gte", dateFromGMTLocalDateTime(dateTime, "GMT")));
        andList.add(tmpObject);
        tmpObject = new BasicDBObject("measures.dateGMT", new BasicDBObject("$lt", dateFromGMTLocalDateTime(dateTime.plusDays(1), "GMT")));
        andList.add(tmpObject);
        tmpObject = new BasicDBObject("$and", andList);
        aggregateList.add(new BasicDBObject("$match", tmpObject));

        // Make the $group object
        tmpObject = new BasicDBObject();
        tmpObject.put("_id", null);
        tmpObject.put("measureCount", new BasicDBObject("$sum", 1));
        aggregateList.add(new BasicDBObject("$group", tmpObject));

        //System.out.println(aggregateList.toString());

        return aggregateList;

        /*DBObject queryObject = new BasicDBObject();
        List<DBObject> aggregateList = new ArrayList<>(3);
        DBObject tmpObject;

        // Make the $match object
        List<DBObject> andList = new ArrayList<>(2);
        tmpObject = new BasicDBObject("startTime", new BasicDBObject("$gte", dateFromGMTLocalDateTime(dateTime, "GMT")));
        andList.add(tmpObject);
        tmpObject = new BasicDBObject("startTime", new BasicDBObject("$lt", dateFromGMTLocalDateTime(dateTime.plusDays(1), "GMT")));
        andList.add(tmpObject);
        tmpObject = new BasicDBObject("$and", andList);
        aggregateList.add(new BasicDBObject("$match", tmpObject));

        // Make the $project object
        tmpObject = new BasicDBObject();
        tmpObject.put("_id", 0);
        tmpObject.put("measureCount", new BasicDBObject("$size", "$measures"));
        aggregateList.add(new BasicDBObject("$project", tmpObject));

        // Make the $group object
        tmpObject = new BasicDBObject("_id", null);
        tmpObject.put("totalCount", new BasicDBObject("$sum", "$measureCount"));
        aggregateList.add(new BasicDBObject("$group", tmpObject));

        //System.out.println(aggregateList.toString());

        return aggregateList;*/
    }

    //public static BasicDBObject makeUserDBObject(String userDir) {
        //return new BasicDBObject("userId", Integer.parseInt(userDir));
    //}

    public void finish() {
        if (this.mongo != null) {
            this.mongo.close();
        }
    }
}
