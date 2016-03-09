package at.ac.tuwien.dbai.sparql.query;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import lombok.Data;

@Data
public class EvalPT {
	private Set<String> outputVars;
	private EvalTreeNode root;

	public EvalPT() {
		outputVars = new HashSet<String>();
	}
	
	public void addOutputVar(String var) {
		outputVars.add(var);
	}
	
	public MappingSet evaluate() {
		MappingSet result = root.evaluate();
		
		for (Mapping m : result) m.project(outputVars);
		
		return result;
	}

}
