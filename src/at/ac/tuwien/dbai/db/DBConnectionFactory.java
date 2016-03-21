package at.ac.tuwien.dbai.db;

/**
 * Created by michael on 21.03.16.
 */
public class DBConnectionFactory {

    public static CommonDBConnection getConnection(DBMetaData.DBType type) {
        switch (type) {
            case DERBY:
                return new DerbyDBConnection(type);
            default:
                return new CommonDBConnection(type);
        }
    }

}
