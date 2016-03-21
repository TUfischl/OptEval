package at.ac.tuwien.dbai.sparql.query;

import java.util.HashSet;
import java.util.Set;

import at.ac.tuwien.dbai.db.DBManager;
import lombok.Data;

@Data
public class EvalPT {

	public enum EvaluationType {
		ITERATIVE, DB
	}

	private Set<String> outputVars;
	private EvalTreeNode root;

	public EvalPT() {
		outputVars = new HashSet<>();
	}
	
	public void addOutputVar(String var) {
		outputVars.add(var);
	}
	
	public MappingSet evaluate(EvaluationType type) {
		switch (type) {
			case ITERATIVE:
				return this.iterativeEvaluation();
			case DB:
				return this.dbEvaluation();
			default:
				return null;
		}
	}

	private MappingSet iterativeEvaluation() {
		MappingSet result = root.evaluate();
		for (Mapping m : result) m.project(outputVars);
		return result;
	}

    private MappingSet dbEvaluation() {
        DBManager manager = new DBManager(this);
        return manager.evaluate();
    }

	public void setRoot(EvalTreeNode root) {
		this.root = root;
	}

	public EvalTreeNode getRoot() {
		return root;
	}

    public Set<String> getOutputVars() {
        return outputVars;
    }
}
