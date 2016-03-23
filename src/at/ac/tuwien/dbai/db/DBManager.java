package at.ac.tuwien.dbai.db;

import at.ac.tuwien.dbai.sparql.query.EvalPT;
import at.ac.tuwien.dbai.sparql.query.EvalTreeNode;
import at.ac.tuwien.dbai.sparql.query.MappingSet;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class DBManager {
    private EvalPT evalPT;
    private StringBuilder select;
    private CommonDBConnection dbConnection;
    private HashMap<String, HashSet<String>> joinCols;
    private Boolean useIndices;

    public DBManager(EvalPT evalPT, DBMetaData.DBType type, Boolean useIndices) {
        this.evalPT = evalPT;
        this.dbConnection = DBConnectionFactory.getConnection(type);
        this.useIndices = useIndices;
        this.joinCols = new HashMap<>();
        joinCols.put(evalPT.getRoot().getId(), new HashSet<>());
        evalPT.getRoot().getChildren().forEach(this::findJoinCols);
    }

    private void findJoinCols(EvalTreeNode node) {
        EvalTreeNode root = evalPT.getRoot();

        HashSet<String> varsForNode = joinCols.get(node.getId());
        if (varsForNode == null) {
            varsForNode = new HashSet<>();
            joinCols.put(node.getId(), varsForNode);
        }

        HashSet<String> equalVars = new HashSet<>(root.getLocalVars());
        Set<String> nodeVars = node.getLocalVars();
        equalVars.retainAll(nodeVars);

        //Add to node
        varsForNode.addAll(equalVars);

        //Add to root
        HashSet<String> rootVars = joinCols.get(root.getId());
        rootVars.addAll(equalVars);

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
        HashSet<String> equalVars = joinCols.get(node.getId());
        for (String tmpCol : equalVars) {
            String col = tmpCol.substring(1);
            String indexName = "eval_index_" + node.getId() + "_" + col;
            dbConnection.createIndex(node.getId(), col, indexName);
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
        HashSet<String> equalCols = joinCols.get(node.getId());
        StringBuilder sb = new StringBuilder();

        int i = 0;
        for (String varTemp : equalCols) {
            String var = varTemp.substring(1);
            sb.append(evalPT.getRoot().getId())
                    .append(".")
                    .append(var)
                    .append("=")
                    .append(node.getId())
                    .append(".")
                    .append(var)
                    .append(" ");
            if (i != equalCols.size() - 1) {
                sb.append("AND ");
            }
            i++;
        }
        return sb.toString();
    }
}
