package at.ac.tuwien.dbai.sparql.query;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(of={"root","prefix","projVars","allProjVars"})
@Data
public class PatternTree implements Query {
    TreeNode root;
	Map<String,String> prefix;
	Set<String> projVars = null;
	boolean allProjVars = false;
	boolean varsModified = true;
	
	public PatternTree(){
		prefix = new HashMap<String,String>();
		prefix.put(Prefix.RDF.getPrefix(), Prefix.RDF.getUri());
		projVars = new LinkedHashSet<String>();
	}
	
	public PatternTree(PatternTree pt) {
		this.setRoot(new TreeNode(pt.root));
		allProjVars = pt.allProjVars;
		prefix = pt.prefix;
		projVars = new HashSet<String>(pt.projVars);
	}

	public PatternTree(Set<Triple> bgp, Set<String> vars, Map<String, String> prefix) {
		this.setRoot(new TreeNode(bgp));
		projVars = new HashSet<String>(vars);
		projVars.retainAll(root.getVars());
		this.prefix = prefix;
	}

	public void addPrefix (String ID, String URI) {
		prefix.put(ID, URI);
	}
	
	public void addProjVars (String s) {
		if (s.startsWith("?")) {
		    projVars.add(s);
		} else if (s == "*") allProjVars = true;
	}
	
	public String getDefaultNameSpace() {
		return prefix.get("") + "#";
	}
	
	@Override
	public String toString() {
		if (selectStar())
		    return toString(null);
		else
			return toString(projVars);
	}
	
	public String toString(Set<String> vars) {
		// TODO Auto-generated method stub
        String s = "";
		
		for (Entry<String,String> e : prefix.entrySet()) {
			s += "PREFIX " + Prefix.toString(e) + " ";
		}
		
		s += "SELECT ";
		if (vars == null)
			s += "* ";
		else
			for (String v : vars) s+= v + " ";
	    
		
		s += "WHERE { ";
		s += root.toString();
		s += "}";
		
		return s;
	}
	
	@Override
	public Set<String> getVars() {
		if (varsModified) {
			root.updateVars();
			varsModified = false;
		}
		return new HashSet<String>(root.getVars());
	}
	
	public Set<String> getProjVars() {
		if (selectStar()) return getVars();
		return new HashSet<String>(projVars);
	}

	@Override
	public String getQuery() {
		return toString();
	}
	
	@Override
	public int getType() {
		return Query.QUERY_PT;
	}
	
	public boolean hasOptional() {
		return root.hasChildren();
	}
	
	public boolean selectStar() {
		return allProjVars;
	}
	
	@Override
	public String getDefaultPrefix() {
		return prefix.get("");
	}
	
	/*protected IRI getIRI(String s) {
		return IRI.create(prefix.get(s.substring(0, s.indexOf(':'))) + "#" + s.substring(s.indexOf(':')+1));
	}*/

	@Override
	public int count() {
		return 1;
	}
	
	/*@Override
	public boolean equals (Object o) {
		if (o.getClass() == this.getClass()) {
			PatternTree t = (PatternTree)o;
			if (t.allProjVars == this.allProjVars)
				if (t.prefix.equals(this.prefix))
					if (t.proj_vars.equals(this.proj_vars))
						if (t.root.equals(this.root))
			               return true;
		}
		return false;
	}*/
}
