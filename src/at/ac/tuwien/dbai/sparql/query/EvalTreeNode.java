package at.ac.tuwien.dbai.sparql.query;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

public class EvalTreeNode {
    private MappingSet mappings;
    private List<EvalTreeNode> children;
    private HashSet<String> localVars;
    private int id;

    public EvalTreeNode(int id) {
        mappings = new MappingSet();
        children = new LinkedList<EvalTreeNode>();
        localVars = new HashSet<>();
        this.id = id;
    }

    private EvalTreeNode() {}

    public String getId() {
        return "T" + id;
    }

    public void addMapping(Mapping m) {
        mappings.add(m);
    }

    public void addChildNode(EvalTreeNode n) {
        children.add(n);
    }

    public MappingSet evaluate() {
        return evaluate(null);
    }

    public MappingSet evaluate(MappingSet results) {
        if (results == null) {
            results = new MappingSet();
            results.addAll(mappings);
        } else {
            results.extend(mappings);
        }
        for (EvalTreeNode child : children) {
            results = child.evaluate(results);
        }
        return results;
    }

    public MappingSet getMappings() {
        return mappings;
    }

    public List<EvalTreeNode> getChildren() {
        return children;
    }

    public HashSet<String> getLocalVars() {
        return localVars;
    }

    public void setLocalVars(HashSet<String> localVars) {
        this.localVars = localVars;
    }

}
