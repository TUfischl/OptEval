package at.ac.tuwien.dbai.db;

import java.sql.SQLException;


public class DerbyDBConnection extends CommonDBConnection {

    public DerbyDBConnection(DBMetaData.DBType type) {
        super(type);
    }

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
}
