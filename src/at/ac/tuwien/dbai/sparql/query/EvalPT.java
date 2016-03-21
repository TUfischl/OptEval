package at.ac.tuwien.dbai.sparql.query;

import at.ac.tuwien.dbai.db.DBManager;
import at.ac.tuwien.dbai.db.DBMetaData;
import lombok.Data;

import java.util.HashSet;
import java.util.Set;

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
	
	public MappingSet evaluate(EvaluationType type, DBMetaData.DBType dbType) {
		switch (type) {
			case ITERATIVE:
				return this.iterativeEvaluation();
			case DB:
				return this.dbEvaluation(dbType);
			default:
				return null;
		}
	}

	private MappingSet iterativeEvaluation() {
		MappingSet result = root.evaluate();
		for (Mapping m : result) m.project(outputVars);
		return result;
	}

    private MappingSet dbEvaluation(DBMetaData.DBType dbType) {
        DBManager manager = new DBManager(this, dbType);
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
