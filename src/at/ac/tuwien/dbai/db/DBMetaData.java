package at.ac.tuwien.dbai.db;

public class DBMetaData {

    public enum DBType {
        H2, HSQLDB, DERBY
    }

    private String driver;
    private String connection;
    private String user;
    private String password;
    private DBType type;

    public static DBMetaData getMetaData(DBType type) {
        DBMetaData meta = new DBMetaData();
        meta.type = type;
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
            case DERBY:
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

    public DBType getType() {
        return type;
    }

    public void loadClass() throws ClassNotFoundException {
        Class.forName(this.getDriver());
    }

    public static void preLoadClasses() throws ClassNotFoundException {
        for (DBMetaData.DBType dbType : DBMetaData.DBType.values()) {
            DBMetaData metaData = DBMetaData.getMetaData(dbType);
            metaData.loadClass();
        }
    }
}
