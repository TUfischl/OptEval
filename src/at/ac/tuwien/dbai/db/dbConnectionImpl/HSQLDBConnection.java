package at.ac.tuwien.dbai.db.dbConnectionImpl;

import at.ac.tuwien.dbai.db.CommonDBConnection;

import java.sql.SQLException;

/**
 * Created by michael on 04.04.16.
 */
public class HSQLDBConnection extends CommonDBConnection {
    private static final String DRIVER      = "org.hsqldb.jdbcDriver";
    private static final String CONNECTION  = "jdbc:hsqldb:mem:.";
    private static final String USER        = "SA";
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
    public void deleteData() throws SQLException {
        executeDDLStatement("DROP SCHEMA PUBLIC CASCADE");
    }
}
