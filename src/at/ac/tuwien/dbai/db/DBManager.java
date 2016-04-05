package at.ac.tuwien.dbai.db;

import at.ac.tuwien.dbai.sparql.query.EvalPT;
import at.ac.tuwien.dbai.sparql.query.EvalTreeNode;
import at.ac.tuwien.dbai.sparql.query.MappingSet;
import org.apache.commons.lang3.builder.EqualsBuilder;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * DBManger contains the logic part of DB algorithms.
 */
public class DBManager {
    private EvalPT evalPT;
    private StringBuilder select;
    private CommonDBConnection dbConnection;
    private HashMap<String, HashSet<TableCol>> joinCols;
    private Boolean useIndices;

    /**
     * TableCol is a helper class only used in DBManger.
     * It represents a pair of table name and column name used in SQL Statements e.g. Employee.name
     */
    private class TableCol {
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

    /**
     * At the initialization a concrete DB connection is created depending on db type.
     * @param evalPT contains data used in algorithm
     * @param type db algorithm to use
     * @param useIndices create indices if true
     */
    public DBManager(EvalPT evalPT, DBConnectionFactory.DBType type, Boolean useIndices) {
        this.evalPT = evalPT;
        this.dbConnection = DBConnectionFactory.getConnection(type);
        this.useIndices = useIndices;
        this.joinCols = new HashMap<>();
    }

    /**
     * Step 1 find join columns (used in create table and create indices)
     * Step 2 create tables and parallel generate select statement
     * Step 3 execute select statement and retrieve data from DB
     * Step 4 clean up DB and shutdown
     * @return result of evaluation
     */
    public MappingSet evaluate() {
        MappingSet set = null;
        try {
            findJoinCols(evalPT.getRoot());
            createAllTables();
            set = dbConnection.select(select.toString());
            cleanUp();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return set;
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

    private void createAllTables() throws SQLException {
        initSelectStatement();
        createSingleTable(evalPT.getRoot());
    }

    private void initSelectStatement() {
        select = new StringBuilder();
        select.append("SELECT ")
                .append(stringFromVars(evalPT.getOutputVars()))
                .append("\nFROM ")
                .append(evalPT.getRoot().getId());
    }

    private void createSingleTable(EvalTreeNode node) throws SQLException {
        String tableName = node.getId();

        dbConnection.dropTableIfExists(tableName);
        dbConnection.createTable(tableName, node.getLocalVars());
        if (useIndices) createIndicesForNode(node);
        dbConnection.insertIntoTable(tableName, node.getLocalVars(), node.getMappings());

        for (EvalTreeNode child : node.getChildren()) {
            appendSelectStatement(child);
            this.createSingleTable(child);
        }
    }

    private void appendSelectStatement(EvalTreeNode child) {
        String childTableName = child.getId();
        select.append("\nLEFT OUTER JOIN ")
                .append(childTableName)
                .append("\n\ton ")
                .append(onClause(child));
    }


    private void cleanUp() throws SQLException {
        dbConnection.deleteData();
        dbConnection.shutdown();
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
