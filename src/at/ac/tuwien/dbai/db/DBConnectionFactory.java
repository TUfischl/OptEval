package at.ac.tuwien.dbai.db;

import at.ac.tuwien.dbai.db.dbConnectionImpl.DerbyDBConnection;
import at.ac.tuwien.dbai.db.dbConnectionImpl.H2DBConnection;
import at.ac.tuwien.dbai.db.dbConnectionImpl.HSQLDBConnection;

/**
 * Created by michael on 21.03.16.
 */
public class DBConnectionFactory {

    public enum DBType {
        H2, HSQLDB, DERBY
    }

    public static CommonDBConnection getConnection(DBType type) {
        switch (type) {
            case DERBY:
                return new DerbyDBConnection();
            case H2:
                return new H2DBConnection();
            case HSQLDB:
                return new HSQLDBConnection();
        }
        return null;
    }

}
