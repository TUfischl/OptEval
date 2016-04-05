package at.ac.tuwien.dbai.db.dbConnectionImpl;

import at.ac.tuwien.dbai.db.CommonDBConnection;

import java.sql.SQLException;


public class DerbyDBConnection extends CommonDBConnection {
    private static final String DRIVER      = "org.apache.derby.jdbc.EmbeddedDriver";
    private static final String CONNECTION  = "jdbc:derby:memory:myDB;create=true";
    private static final String USER        = "";
    private static final String PASSWORD    = "";


    @Override
    protected String getDriver() {
        return DRIVER;
    }

    @Override
    protected String getConnection() {
        return CONNECTION;
    }

    @Override
    protected String getUser() {
        return USER;
    }

    @Override
    protected String getPassword() {
        return PASSWORD;
    }

    @Override
    public void dropTableIfExists(String name) throws SQLException {
        String dropTableSQL = "DROP TABLE " + name + "";
        try {
            super.executeDDLStatement(dropTableSQL);
        } catch (SQLException e) {
            if (!e.getSQLState().equals("42Y55")) { //Derby does not support 'IF EXISTS'
                return; // That's OK
            }
            throw e;
        }
    }

    @Override
    public void deleteData() throws SQLException {
        //no need to
    }

    @Override
    public void shutdown() throws SQLException {
        //no need to
    }
}
