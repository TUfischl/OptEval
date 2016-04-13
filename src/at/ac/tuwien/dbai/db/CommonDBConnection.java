package at.ac.tuwien.dbai.db;

import at.ac.tuwien.dbai.sparql.query.Mapping;
import at.ac.tuwien.dbai.sparql.query.MappingSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.Set;

/**
 * CommonDBConnection provides all functionality to access the DB.
 * It has to be extended by specific DB connection.
 */
public abstract class CommonDBConnection {
    static final Logger logger = LogManager.getLogger(CommonDBConnection.class.getName());
    private final int batchSize = 5000;
    private final int varcharSize = 200; //maybe has to be adapted

    //Use this abstract methods in subclass to specify DB connection
    protected abstract String getDriver();
    protected abstract String getConnection();
    protected abstract String getUser();
    protected abstract String getPassword();

    /**
     * Returns a Connection object for specific DB
     * @return a Connection object for specific DB
     */
    Connection getDBConnection() {
        Connection dbConnection = null;
        try {
            Class.forName(getDriver());
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage());
        }
        try {
            dbConnection = DriverManager.getConnection(getConnection(), getUser(), getPassword());
        } catch (SQLException e) {
            logger.error(e.getMessage());
        }
        return dbConnection;
    }

    /**
     * Queries a select statement and returns a MappingSet
     * @param selectSql
     * @return a MappingSet by querying selectSql
     * @throws SQLException
     */
    public MappingSet select(String selectSql) throws SQLException {
        Connection dbConnection = null;
        Statement statement = null;
        MappingSet mappingSet = null;

        try {
            dbConnection = getDBConnection();
            statement = dbConnection.createStatement();
            logger.info("Query: " + selectSql);
            ResultSet resultSet = statement.executeQuery(selectSql);
            logger.info("Query: finished");
            mappingSet = convertTable(resultSet);
        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            if (statement != null) statement.close();
            if (dbConnection != null) dbConnection.close();
        }
        return mappingSet;
    }

    /**
     * Converts a ResultSet into a MappingSet
     * @param resultSet
     * @return converted MappingSet
     * @throws SQLException
     */
    private MappingSet convertTable(ResultSet resultSet) throws SQLException {
        MappingSet mappingSet = new MappingSet();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnsNumber = metaData.getColumnCount();

        while (resultSet.next()) {
            Mapping m = new Mapping();
            for (int i = 1; i <= columnsNumber; i++) {
                String columnValue = resultSet.getString(i);
                if (columnValue != null) {
                    m.add("?" + metaData.getColumnName(i), columnValue);
                }
            }
            mappingSet.add(m);
        }
        return mappingSet;
    }

    /**
     * Inserts mappingSet in given table.
     * Using batch insert.
     * @param name table name
     * @param vars name of columns
     * @param mappingSet data to insert
     * @throws SQLException
     */
    public void insertIntoTable(String name, Set<String> vars, MappingSet mappingSet) throws SQLException {
        logger.info("Insert into " + name);
        Connection dbConnection = null;
        PreparedStatement ps = null;

        //generate insert statement
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
            ps = dbConnection.prepareStatement(sql);

            //using batch insert
            int count = 0;
            for (Mapping mapping : mappingSet) {
                for (int i = 0; i < cols.length; i++) {
                    String value = mapping.getMap().get(cols[i]);
                    ps.setString(i + 1, value);
                }
                ps.addBatch();
                if (++count % batchSize == 0) {
                    ps.executeBatch();
                    logger.info("executeBatch for " + name + ": " + count + " - " + mappingSet.size());
                }
            }
            ps.executeBatch(); // insert remaining records
            logger.info("executeBatch for " + name + ": finished");
        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            if (ps != null) ps.close();
            if (dbConnection != null) dbConnection.close();
        }
    }

    /**
     * Creates a table.
     * Each column is created as a VARCHAR.
     * No keys are created.
     * @param name table name
     * @param cols columns in table
     * @throws SQLException
     */
    public void createTable(String name, Set<String> cols) throws SQLException {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("CREATE TABLE ").append(name).append("(");
        int i = 0;

        for (String var : cols) {
            stringBuilder.append(var.substring(1)).append(" VARCHAR(").append(varcharSize).append(")");
            if (i != cols.size() - 1) {
                stringBuilder.append(", ");
            } else {
                stringBuilder.append(")");
            }
            i++;
        }

        executeDDLStatement(stringBuilder.toString());
    }

    /**
     * Creates an index on given table and column
     * @param table table name
     * @param column
     * @param indexName
     * @throws SQLException
     */
    public void createIndex(String table, String column, String indexName) throws SQLException {
        String createIndexSQL = "CREATE INDEX " + indexName + " ON " + table + "(" + column + ")";
        executeDDLStatement(createIndexSQL);
    }

    /**
     * Drops an specific index
     * @param table table name
     * @param indexName
     * @throws SQLException
     */
    public void dropIndex(String table, String indexName) throws SQLException {
        String createIndexSQL = "DROP INDEX IF EXISTS " + table + "." + indexName;
        executeDDLStatement(createIndexSQL);
    }

    /**
     * Drops a table by name
     * @param name table name
     * @throws SQLException
     */
    public void dropTableIfExists(String name) throws SQLException {
        String dropTableSQL = "DROP TABLE IF EXISTS " + name + ";";
        executeDDLStatement(dropTableSQL);
    }

    /**
     * Helper method to execute DDL statement
     * @param ddlStatment
     * @throws SQLException
     */
    protected void executeDDLStatement(String ddlStatment) throws SQLException {
        logger.info(ddlStatment);
        Connection dbConnection = null;
        Statement statement = null;

        try {
            dbConnection = getDBConnection();
            statement = dbConnection.createStatement();
            statement.execute(ddlStatment);
        } catch (SQLException e) {
            logger.error(e.getMessage());
        } finally {
            if (statement != null) statement.close();
            if (dbConnection != null) dbConnection.close();
        }
    }

    /**
     * Removes all data from DB
     * @throws SQLException
     */
    public void deleteData() throws SQLException {
        executeDDLStatement("DROP ALL OBJECTS DELETE FILES");
    }

    /**
     * Explicit shut down of DB
     * @throws SQLException
     */
    public void shutdown() throws SQLException {
        executeDDLStatement("SHUTDOWN");
    }
}
