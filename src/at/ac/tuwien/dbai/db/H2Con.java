package at.ac.tuwien.dbai.db;

import at.ac.tuwien.dbai.sparql.query.Mapping;
import at.ac.tuwien.dbai.sparql.query.MappingSet;

import java.io.PrintStream;
import java.sql.*;
import java.util.Map;

/**
 * Created by michael on 14.03.16.
 */
public class H2Con {
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
        MappingSet mappingSet = new MappingSet();
        try {
            dbConnection = getDBConnection();
            statement = dbConnection.createStatement();
            //System.out.println(selectSql);
            ResultSet resultSet = statement.executeQuery(selectSql);
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
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        } finally {
            if (statement != null) statement.close();
            if (dbConnection != null) dbConnection.close();
        }
        return null;
    }

    public static void insertIntoTable(String name, String[] cols, MappingSet mappingSet) throws SQLException {
        Connection dbConnection = null;
        Statement statement = null;

        StringBuilder sbcols = new StringBuilder();
        StringBuilder sbValues = new StringBuilder();
        sbcols.append("INSERT INTO ").append(name).append("(");
        sbValues.append(" VALUES(");
        for (int i = 0; i < cols.length; i++) {
            sbcols.append(cols[i].substring(1));
            sbValues.append("?");
            if (i != cols.length - 1) {
                sbcols.append(", ");
                sbValues.append(", ");
            } else {
                sbcols.append(")");
                sbValues.append(")");
            }
        }
        sbcols.append(sbValues);
        String sql = sbcols.toString();

        try {
            dbConnection = getDBConnection();
            PreparedStatement ps = dbConnection.prepareStatement(sql);

            final int batchSize = 500;
            int count = 0;

            for (Mapping mapping: mappingSet) {
                for (int i = 0; i < cols.length; i++) {
                    String value = mapping.getMap().get(cols[i]);
                    ps.setString(i+1, value);
                }

                ps.addBatch();

                if(++count % batchSize == 0) {
                    ps.executeBatch();
                }
            }
            ps.executeBatch(); // insert remaining records
            //System.out.println("Inserted MappingSet");
        } catch (SQLException e) {
            System.err.println(e.getMessage());
        } finally {
            if (statement != null) statement.close();
            if (dbConnection != null) dbConnection.close();
        }
    }

    public static void createTable(String name, String[] cols) throws SQLException {
        Connection dbConnection = null;
        Statement statement = null;

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE TABLE ").append(name).append("(");
        for (int i = 0; i < cols.length; i++) {
            stringBuilder.append(cols[i].substring(1)).append(" VARCHAR(200)");
            if (i != cols.length - 1) {
                stringBuilder.append(", ");
            } else {
                stringBuilder.append(")");
            }
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
