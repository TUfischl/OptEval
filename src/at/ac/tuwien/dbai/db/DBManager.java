package at.ac.tuwien.dbai.db;

import at.ac.tuwien.dbai.sparql.query.EvalPT;
import at.ac.tuwien.dbai.sparql.query.EvalTreeNode;
import at.ac.tuwien.dbai.sparql.query.MappingSet;
import org.apache.commons.lang3.builder.EqualsBuilder;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class DBManager {
    private EvalPT evalPT;
    private StringBuilder select;
    private CommonDBConnection dbConnection;
    private HashMap<String, HashSet<TableCol>> joinCols;
    private Boolean useIndices;


    public class TableCol {
        private String col;
        private String table;

        public TableCol(String col, String table) {
            this.col = col;
            this.table = table;
        }

        @Override
        public String toString() {
            return table + "." + col.substring(1);
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TableCol))
                return false;
            if (obj == this)
                return true;

            TableCol rhs = (TableCol) obj;
            return new EqualsBuilder().
                    append(col, rhs.col).
                    append(table, rhs.table).
                    isEquals();
        }
    }

    public DBManager(EvalPT evalPT, DBConnectionFactory.DBType type, Boolean useIndices) {
        this.evalPT = evalPT;
        this.dbConnection = DBConnectionFactory.getConnection(type);
        this.useIndices = useIndices;

        this.joinCols = new HashMap<>();
        findJoinCols(evalPT.getRoot());
    }

    private void findJoinCols(EvalTreeNode node) {
        HashSet<TableCol> varsForNode = joinCols.get(node.getId());
        if (varsForNode == null) {
            varsForNode = new HashSet<>();
            joinCols.put(node.getId(), varsForNode);
        }

        EvalTreeNode parent = node.getParent();
        if (parent != null) {
            HashSet<String> equalVars = new HashSet<>(parent.getLocalVars());
            Set<String> nodeVars = node.getLocalVars();
            equalVars.retainAll(nodeVars);

            for (String col : equalVars) {
                varsForNode.add(new TableCol(col, parent.getId()));
            }
        }
        node.getChildren().forEach(this::findJoinCols);
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
        createSingleTable(evalPT.getRoot());
    }

    private void createSingleTable(EvalTreeNode node) throws SQLException {
        String tableName = node.getId();

        dbConnection.dropTableIfExists(tableName);
        dbConnection.createTable(tableName, node.getLocalVars());
        if (useIndices) createIndicesForNode(node);
        dbConnection.insertIntoTable(tableName, node.getLocalVars(), node.getMappings());

        for (EvalTreeNode child : node.getChildren()) {
            String childTableName = child.getId();
            select.append("\nLEFT OUTER JOIN ")
                    .append(childTableName)
                    .append("\n\ton ")
                    .append(onClause(child));

            this.createSingleTable(child);
        }
    }

    private void createIndicesForNode(EvalTreeNode node) throws SQLException {
        HashSet<TableCol> equalVars = joinCols.get(node.getId());
        for (TableCol tableCol : equalVars) {
            String col = tableCol.col.substring(1);
            String indexName = "eval_index_" + node.getId() + "_" + col;
            dbConnection.createIndex(node.getId(), col, indexName);

            String indexNameParent = "eval_index_" + tableCol.table + "_" + col;
            dbConnection.createIndex(tableCol.table, col, indexNameParent);
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
        HashSet<TableCol> equalCols = joinCols.get(node.getId());
        StringBuilder sb = new StringBuilder();

        int i = 0;
        for (TableCol tableCol : equalCols) {
            String rhs = tableCol.toString();
            sb.append(node.getId())
                    .append(".")
                    .append(tableCol.col.substring(1))
                    .append("=")
                    .append(rhs)
                    .append(" ");
            if (i != equalCols.size() - 1) {
                sb.append("AND ");
            }
            i++;
        }
        return sb.toString();
    }
}
