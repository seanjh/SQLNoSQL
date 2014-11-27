package DatabaseHelper;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class QueryHelper {
    private static ClassLoader classLoader = QueryHelper.class.getClassLoader();

    public static String getQueryStatementFromFile(String filename) throws IOException {
        URL url = classLoader.getResource(filename);
        Path path = Paths.get(url.getPath());
        StringBuilder sqlStatement = new StringBuilder();
        List<String> lines = new LinkedList<>();

        try {
            lines = Files.readAllLines(path, Charset.defaultCharset());
        } catch (IOException e) { e.printStackTrace(); }

        for (String s : lines) {
            sqlStatement.append(s);
        }

        return sqlStatement.toString();
    }

    public static String completeStatement(String rootStatement, int paramCount) {
        StringBuilder result = new StringBuilder(rootStatement);
        result.append(" (");
        for (int i = 0; i < paramCount; i++) {
            result.append("?");
            if (i != paramCount - 1) {
                result.append(",");
            }
        }
        result.append(")");
        return result.toString();
    }

    public static String getRowAsString(ResultSet result, ResultSetMetaData meta)
            throws SQLException {
        int columns = meta.getColumnCount();
        String columnName;
        String value;
        StringBuilder row = new StringBuilder();
        for (int i = 1; i <= columns; i++) {
            columnName = meta.getColumnName(i);
            value = result.getString(i);
            row.append(String.format("\t%s=%s\n", columnName, value));
        }
        return row.toString();
    }

    public static void setParameterList(PreparedStatement stmt,
                                        Iterator<Integer> parameterValues,
                                        int size)
            throws SQLException {
        // Parameters are 1-indexed (arg)
        for (int i = 1;  i <= size; i++) {
            stmt.setInt(i, parameterValues.next());
        }
    }
}

