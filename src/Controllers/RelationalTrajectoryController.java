package Controllers;

import CreateDB.Bootstrappers.BootstrapMySQL;
import DatabaseHelper.QueryHelper;

import java.io.IOException;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Date;

public class RelationalTrajectoryController extends RelationalController {
    private static final String trajectoryCountFileName = "Trajectory/TrajectorySetCount.sql";
    private static final String measuresOnDateCountFileName = "Trajectory/MeasuresOnDate.sql";

    public RelationalTrajectoryController() { super(); }

    public void loadTrajectoryDatabase(String sourcePath) throws IOException {
        try {
            BootstrapMySQL.bootstrapTrajectories(getRelationalConnection(), sourcePath);
        }
        catch (SQLException e) { e.printStackTrace(); }
    }

    public static Iterator<Map<String, Object>> getTrajectorySetCount(Connection conn,
                                                                      String dateTimeFileName, int userId) throws SQLException {
        String statementStr = "";
        try {
            statementStr = QueryHelper.getQueryStatementFromFile(trajectoryCountFileName);
        } catch (IOException e) { e.printStackTrace(); }

        List<Map<String, Object>> results = new LinkedList<>();
        Map<String, Object> oneResult;

        // Try to load the connection
        try (PreparedStatement stmt = conn.prepareStatement(statementStr)) {
            stmt.setString(1, BootstrapMySQL.getSQLDateTimeStringFromDateTimeFileName(dateTimeFileName));
            stmt.setInt(2, userId);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    oneResult = new HashMap<>();
                    oneResult.put("MeasureCount", rs.getInt("measureCount"));
                    results.add(oneResult);
                }
            }
        }
        conn.commit();

        return results.iterator();
    }

    public static Iterator<Map<String, Object>> getMeasuresOnDateCount(Connection conn, Date date) throws SQLException {
        Instant instant = Instant.ofEpochMilli(date.getTime());
        LocalDateTime ldt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());

        String statementStr = "";
        try {
            statementStr = QueryHelper.getQueryStatementFromFile(measuresOnDateCountFileName);
        } catch (IOException e) { e.printStackTrace(); }

        List<Map<String, Object>> results = new LinkedList<>();
        Map<String, Object> oneResult;

        // Try to load the connection
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        try (PreparedStatement stmt = conn.prepareStatement(statementStr)) {
            stmt.setString(1, ldt.format(formatter));
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    oneResult = new HashMap<>();
                    oneResult.put("MeasureCount", rs.getInt("measureCount"));
                    results.add(oneResult);
                }
            }
        }
        conn.commit();

        return results.iterator();
    }
}
