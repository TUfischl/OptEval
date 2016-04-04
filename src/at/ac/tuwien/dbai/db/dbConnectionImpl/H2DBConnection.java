package at.ac.tuwien.dbai.db.dbConnectionImpl;

import at.ac.tuwien.dbai.db.CommonDBConnection;

/**
 * Created by michael on 04.04.16.
 */
public class H2DBConnection extends CommonDBConnection {
    private static final String DRIVER      = "org.h2.Driver";
    private static final String CONNECTION  = "jdbc:h2:~/test";
    private static final String USER        = "sa";
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
}
