package Controllers;

import DatabaseHelper.MySQLProperties;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;


public class RelationalController {
    private MysqlDataSource relationalDataSource = null;
    private Connection relationalConn = null;

    public RelationalController() {
        try {
            this.relationalDataSource = MySQLProperties.getRelationalDataSource();
        } catch (IOException e) {e.printStackTrace(); }
    }

    public MysqlDataSource getRelationalDataSource() {
        return this.relationalDataSource;
    }

    public Connection getRelationalConnection() {
        try {
            if (this.relationalConn == null) {
                this.relationalConn = this.relationalDataSource.getConnection();
                this.relationalConn.setAutoCommit(false);
            }
        } catch (SQLException e) { e.printStackTrace(); }
        return this.relationalConn;
    }

    public void setRelationalDatabase(String databaseName) {
        try {
            if (this.relationalConn != null && !this.relationalConn.isClosed())
                this.relationalConn.close();
            this.relationalDataSource.setDatabaseName(databaseName);
            this.relationalConn = this.relationalDataSource.getConnection();
            this.relationalConn.setAutoCommit(false);
        } catch (SQLException e) { e.printStackTrace(); }
    }

    public void finish() {
        try {
            if (this.relationalConn != null)
                this.relationalConn.close();
        } catch (SQLException e) { e.printStackTrace(); }
    }
}
