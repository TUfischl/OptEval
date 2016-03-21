package at.ac.tuwien.dbai.db;

import at.ac.tuwien.dbai.sparql.query.EvalPT;
import at.ac.tuwien.dbai.sparql.query.EvalTreeNode;
import at.ac.tuwien.dbai.sparql.query.MappingSet;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

public class DBManager {
    private EvalPT evalPT;
    private StringBuilder select;
    private CommonDBConnection dbConnection;

    public DBManager(EvalPT evalPT, DBMetaData.DBType type) {
        this.evalPT = evalPT;
        this.dbConnection = DBConnectionFactory.getConnection(type);
    }

    public MappingSet evaluate() {
        MappingSet set = null;
        try {
            createAllTables();
            set = dbConnection.select(select.toString());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return set;
    }

    private void createAllTables() throws SQLException {
        select = new StringBuilder();
        select.append("SELECT ")
                .append(stringFromVars(evalPT.getOutputVars()))
                .append("\nFROM ")
                .append(evalPT.getRoot().getId());
        createSingleTable(evalPT.getRoot(), evalPT.getRoot().getId());
    }

    private void createSingleTable(EvalTreeNode node, String tableName) throws SQLException {
        dbConnection.dropTableIfExists(tableName);
        dbConnection.createTable(tableName, node.getLocalVars());
        dbConnection.insertIntoTable(tableName, node.getLocalVars(), node.getMappings());

        for (EvalTreeNode child : node.getChildren()) {
            String childTableName = child.getId();
            select.append("\nLEFT OUTER JOIN ")
                    .append(childTableName)
                    .append("\n\ton ")
                    .append(onClause(child));

            this.createSingleTable(child, childTableName);
        }
    }

    private String stringFromVars(Set<String> set) {
        StringBuilder sb = new StringBuilder();
        int i = 0;

        for (String var : set) {
            sb.append(idFromVar(var, evalPT.getRoot()))
                    .append(".")
                    .append(var.substring(1));
            if (i != set.size() - 1) {
                sb.append(", ");
            }
            i++;
        }
        return sb.toString();
    }

    private String idFromVar(String var, EvalTreeNode node) {
        if (node.getLocalVars().contains(var)) {
            return node.getId();
        }
        for (EvalTreeNode child : node.getChildren()) {
            String tempId = idFromVar(var, child);
            if (tempId != null) return tempId;
        }
        return null;
    }

    private String onClause(EvalTreeNode node) {
        HashSet<String> rootVars = new HashSet<>(evalPT.getRoot().getLocalVars());
        Set<String> nodeVars = node.getLocalVars();
        StringBuilder sb = new StringBuilder();

        rootVars.retainAll(nodeVars);
        int i = 0;
        for (String varTemp : rootVars) {
            String var = varTemp.substring(1);
            sb.append(evalPT.getRoot().getId())
                    .append(".")
                    .append(var)
                    .append("=")
                    .append(node.getId())
                    .append(".")
                    .append(var)
                    .append(" ");
            if (i != rootVars.size() - 1) {
                sb.append("AND ");
            }
            i++;
        }
        return sb.toString();
    }
}
