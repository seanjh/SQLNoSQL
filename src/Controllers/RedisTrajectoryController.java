package Controllers;

import CreateDB.Bootstrappers.BootstrapRedis;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Connection;
import redis.clients.jedis.Client;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class RedisTrajectoryController {
    private static final String hostName = "localhost";
    private Jedis jedis;
    private Connection connection;

    public RedisTrajectoryController() {
        this.jedis = new Jedis(hostName);
        this.connection = this.jedis.getClient();
    }

    public void createDatabase(String sourcePath) throws IOException {
        BootstrapRedis.bootstrap(this.jedis, sourcePath);
    }

    public Jedis getJedis() { return this.jedis; }

    public static Map<String, Object> getMeasuresOnDateCount(Jedis jedis, LocalDate date) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
        List<String> trajset = jedis.lrange("date:" + date.format(formatter), 0, -1);

        Map<String, Object> result = new HashMap<>();
        result.put("measureCount", trajset.size());
        return result;
    }

    public static Map<String, Object> getTrajectorySetCount(Jedis jedis, String dateTimeFileName) {
        List<String> trajset = jedis.lrange("trajset:" + dateTimeFileName.substring(0, 14), 0, -1);

        Map<String, Object> result = new HashMap<>();
        result.put("measureCount", trajset.size());
        return result;
    }

    public void finish() {
        if (this.connection != null)
            this.connection.close();
        if (this.jedis != null)
            this.jedis.close();
    }
}
