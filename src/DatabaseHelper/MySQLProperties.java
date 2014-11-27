package DatabaseHelper;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MySQLProperties {
    public static final String mySQLConfigFileName = "mysql.properties";

    public static MysqlDataSource getRelationalDataSource() throws IOException {
        return MySQLProperties.getRelationalDataSource("");
    }

    public static MysqlDataSource getRelationalDataSource(String databaseName) throws IOException {
        Properties properties = MySQLProperties.loadMySQLProperties();
        MysqlDataSource source = new MysqlDataSource();

        source.setServerName(properties.getProperty("hostname"));
        source.setPort(Integer.parseInt(properties.getProperty("port")));
        if (!databaseName.isEmpty()) {
            source.setDatabaseName(properties.getProperty("database"));
        }
        source.setUser(properties.getProperty("user"));
        if (!properties.getProperty("password").isEmpty()) {
            source.setPassword(properties.getProperty("password"));
        }
        return source;
    }

    public static Properties loadMySQLProperties() throws IOException {
        ClassLoader classloader = Thread.currentThread().getContextClassLoader();
        InputStream is = classloader.getResourceAsStream(mySQLConfigFileName);
        Properties properties = new Properties();
        properties.load(is);

        if (properties.getProperty("hostname") == null ||
                properties.getProperty("hostname").isEmpty()) {
            properties.setProperty("hostname", "localhost");
        }
        if (properties.getProperty("port") == null ||
                properties.getProperty("port").isEmpty()) {
            properties.setProperty("port", "3306");
        }
        if (properties.getProperty("user") == null ||
                properties.getProperty("user").isEmpty()) {
            properties.setProperty("user", "root");
        }

        return properties;
    }
}
