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

    /**
     * Searches a node and recursively all sub nodes to add all join columns.
     * All required join columns are located in the parent node of the designated node
     * @param node
     */
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

    /**
     * Initial call of createSingleTable with root node
     * and setup of the select statement
     * @throws SQLException
     */
    private void createAllTables() throws SQLException {
        initSelectStatement();
        createSingleTable(evalPT.getRoot());
    }

    /**
     * Initial setup of the select statement
     * by adding the root table
     */
    private void initSelectStatement() {
        select = new StringBuilder();
        select.append("SELECT ")
                .append(stringFromVars(evalPT.getOutputVars()))
                .append("\nFROM ")
                .append(evalPT.getRoot().getId());
    }

    /**
     * Sets up one table for a given node by
     * dropping the table,
     * creating the table,
     * create optional indices and
     * inserting data of the given node.
     * Calls itself for all child nodes.
     * @param node
     * @throws SQLException
     */
    private void createSingleTable(EvalTreeNode node) throws SQLException {
        String tableName = node.getId();

        dbConnection.dropTableIfExists(tableName);
        dbConnection.createTable(tableName, node.getLocalVars());
        if (useIndices) createIndicesForNode(node);
        dbConnection.insertIntoTable(tableName, node.getLocalVars(), node.getMappings());

        for (EvalTreeNode child : node.getChildren()) {
            appendSelectStatement(child);
            createSingleTable(child);
        }
    }

    /**
     * Extends the select statement for a given node
     * @param child
     */
    private void appendSelectStatement(EvalTreeNode child) {
        String childTableName = child.getId();
        select.append("\nLEFT OUTER JOIN ")
                .append(childTableName)
                .append("\n\ton ")
                .append(onClause(child));
    }


    /**
     * Cleans up the DB by deleting all data and shutting it down
     * @throws SQLException
     */
    private void cleanUp() throws SQLException {
        dbConnection.deleteData();
        dbConnection.shutdown();
    }

    /**
     * Creates all relevant indices for a given node.
     * Indices are created for all join columns on both relevant tables.
     * @param node
     * @throws SQLException
     */
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

    /**
     * Returns a comma separated list of the variables in given set as a String e.g. T1.X1, T2.X2, T1.X3
     * For each variable the corresponding table has to be found.
     * @param set of variables
     * @return a comma separated list of the variables in given set as a String
     */
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

    /**
     * Returns the corresponding table for a given var and searching recursively all child nodes.
     * @param var to search for
     * @param node to start at
     * @return the corresponding table for a given var and searching recursively all child nodes.
     */
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

    /**
     * Returns the on clause as part of the select statement
     * by combining all join columns of a given node.
     * @param node
     * @return on clause
     */
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
