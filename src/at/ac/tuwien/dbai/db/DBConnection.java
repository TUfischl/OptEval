package at.ac.tuwien.dbai.db;

import at.ac.tuwien.dbai.sparql.query.Mapping;
import at.ac.tuwien.dbai.sparql.query.MappingSet;

import java.io.PrintStream;
import java.sql.*;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by michael on 14.03.16.
 */
public class DBConnection {
    private static final String DB_DRIVER = "org.h2.Driver";
    private static final String DB_CONNECTION = "jdbc:h2:~/test";
    private static final String DB_USER = "sa";
    private static final String DB_PASSWORD = "";

    private static Connection getDBConnection() {
        Connection dbConnection = null;
        try {
            Class.forName(DB_DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println(e.getMessage());
        }
        try {
            dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER,
                    DB_PASSWORD);
            return dbConnection;
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return dbConnection;
    }


    public static MappingSet select(String selectSql) throws SQLException {
        Connection dbConnection = null;
        Statement statement = null;

        try {
            dbConnection = getDBConnection();
            statement = dbConnection.createStatement();
            //System.out.println(selectSql);
            ResultSet resultSet = statement.executeQuery(selectSql);
            return convertTable(resultSet);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        } finally {
            if (statement != null) statement.close();
            if (dbConnection != null) dbConnection.close();
        }
        return null;
    }

    private static MappingSet convertTable(ResultSet resultSet) throws SQLException {
        MappingSet mappingSet = new MappingSet();
        ResultSetMetaData rsmd = resultSet.getMetaData();
        int columnsNumber = rsmd.getColumnCount();

        while (resultSet.next()) {
            Mapping m = new Mapping();
            for (int i = 1; i <= columnsNumber; i++) {
                String columnValue = resultSet.getString(i);
                if (columnValue != null) {
                    m.add("?" + rsmd.getColumnName(i), columnValue);
                }
            }
            mappingSet.add(m);
        }
        return mappingSet;
    }

    public static void insertIntoTable(String name, Set<String> vars, MappingSet mappingSet) throws SQLException {
        System.out.println("Insert into " + name);
        Connection dbConnection = null;
        Statement statement = null;

        String[] cols = vars.toArray(new String[vars.size()]);
        StringBuilder sbCols = new StringBuilder();
        StringBuilder sbValues = new StringBuilder();
        sbCols.append("INSERT INTO ").append(name).append("(");
        sbValues.append(" VALUES(");
        for (int i = 0; i < cols.length; i++) {
            sbCols.append(cols[i].substring(1));
            sbValues.append("?");
            if (i != cols.length - 1) {
                sbCols.append(", ");
                sbValues.append(", ");
            } else {
                sbCols.append(")");
                sbValues.append(")");
            }
        }
        sbCols.append(sbValues);
        String sql = sbCols.toString();

        try {
            dbConnection = getDBConnection();
            PreparedStatement ps = dbConnection.prepareStatement(sql);

            final int batchSize = 1000;
            int count = 0;

            for (Mapping mapping: mappingSet) {
                for (int i = 0; i < cols.length; i++) {
                    String value = mapping.getMap().get(cols[i]);
                    ps.setString(i+1, value);
                }

                ps.addBatch();

                if(++count % batchSize == 0) {
                    ps.executeBatch();
                    System.out.println("executeBatch for " + name + ": " + count + " - " + mappingSet.size());
                }
            }
            ps.executeBatch(); // insert remaining records
            //System.out.println("Inserted MappingSet");
            System.out.println("executeBatch finished for" + name);
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        } finally {
            if (statement != null) statement.close();
            if (dbConnection != null) dbConnection.close();
        }
    }

    public static void createTable(String name, Set<String> cols) throws SQLException {
        Connection dbConnection = null;
        Statement statement = null;

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE TABLE ").append(name).append("(");
        int i = 0;
        for (String var : cols) {
            stringBuilder.append(var.substring(1)).append(" VARCHAR(200)");
            if (i != cols.size() - 1) {
                stringBuilder.append(", ");
            } else {
                stringBuilder.append(")");
            }
            i++;
        }
        String createTableSQL = stringBuilder.toString();

        try {
            dbConnection = getDBConnection();
            statement = dbConnection.createStatement();
            //System.out.println(createTableSQL);
            statement.execute(createTableSQL);
            //System.out.println("Table '" + name + "' is created!");
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        } finally {
            if (statement != null) statement.close();
            if (dbConnection != null) dbConnection.close();
        }
    }

    public static void dropTableIfExists(String name) throws SQLException {
        Connection dbConnection = null;
        Statement statement = null;

        String dropTableSQL = "DROP TABLE IF EXISTS " + name + ";";

        try {
            dbConnection = getDBConnection();
            statement = dbConnection.createStatement();
            //System.out.println(dropTableSQL);
            statement.execute(dropTableSQL);
            //System.out.println("Table '" + name + "' is dropped!");
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        } finally {
            if (statement != null) statement.close();
            if (dbConnection != null) dbConnection.close();
        }
    }
}
