package at.ac.tuwien.dbai.db;

/**
 * Created by michael on 21.03.16.
 */
public class DBMetaData {

    public enum DBType {
        H2, HSQLDB, Derby
    }

    private String driver;
    private String connection;
    private String user;
    private String password;

    public static DBMetaData getMetaData(DBType type) {
        DBMetaData meta = new DBMetaData();
        switch (type) {
            case H2:
                meta.driver = "org.h2.Driver";
                meta.connection = "jdbc:h2:~/test";
                meta.user = "sa";
                meta.password = "";
                break;
            case HSQLDB:
                meta.driver = "org.hsqldb.jdbcDriver";
                meta.connection = "jdbc:hsqldb:mem:.";
                meta.user = "SA";
                meta.password = "";
                break;
            case Derby:
                meta.driver = "org.apache.derby.jdbc.EmbeddedDriver";
                meta.connection = "jdbc:derby:derbyDB;create=true";
                meta.user = "";
                meta.password = "";
                break;
        }
        return meta;
    }

    public String getDriver() {
        return driver;
    }

    public String getConnection() {
        return connection;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }
}
