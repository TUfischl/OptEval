package at.ac.tuwien.dbai.db;

import at.ac.tuwien.dbai.sparql.query.Mapping;
import at.ac.tuwien.dbai.sparql.query.MappingSet;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.*;
import java.util.Set;

public abstract class CommonDBConnection {
    static final Logger logger = LogManager.getLogger(CommonDBConnection.class.getName());

    protected abstract String getDriver();
    protected abstract String getConnection();
    protected abstract String getUser();
    protected abstract String getPassword();

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

    public void insertIntoTable(String name, Set<String> vars, MappingSet mappingSet) throws SQLException {
        logger.info("Insert into " + name);
        Connection dbConnection = null;
        PreparedStatement ps = null;

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

            final int batchSize = 5000;
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

    public void createTable(String name, Set<String> cols) throws SQLException {
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

        executeDDLStatement(stringBuilder.toString());
    }

    public void createIndex(String table, String column, String indexName) throws SQLException {
        String createIndexSQL = "CREATE INDEX " + indexName + " ON " + table + "(" + column + ")";
        executeDDLStatement(createIndexSQL);
    }

    public void dropIndex(String table, String indexName) throws SQLException {
        String createIndexSQL = "DROP INDEX IF EXISTS " + table + "." + indexName;
        executeDDLStatement(createIndexSQL);
    }

    public void dropTableIfExists(String name) throws SQLException {
        String dropTableSQL = "DROP TABLE IF EXISTS " + name + ";";
        executeDDLStatement(dropTableSQL);
    }

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
}
