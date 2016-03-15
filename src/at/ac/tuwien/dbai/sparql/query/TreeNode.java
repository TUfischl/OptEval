package at.ac.tuwien.dbai.sparql.query;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(of={"bgp","children"})
@Data
public class TreeNode {
   private List<TreeNode> children;
   private Set<Triple> bgp;
   private Set<String> vars;
   
   public TreeNode () {
	   children = new LinkedList<TreeNode>();
	   bgp = new HashSet<Triple>();
	   vars = new LinkedHashSet<String>();
   }
   
   public TreeNode(TreeNode tn) {
	   this();
	   for (TreeNode c : tn.children) children.add(new TreeNode(c));
	   for (Triple t : tn.bgp) bgp.add(new Triple(t));
	   vars.addAll(tn.vars);
   }

   public TreeNode(Set<Triple> bgp) {
	   this.bgp = new HashSet<Triple>(bgp);
	   children = new LinkedList<TreeNode>();
	   vars = new LinkedHashSet<String>();
	   updateVars();
   }

public void addTriple(String s, String p, String o) { 
	   if (p.equals("a")) p = "rdf:type";
	   Triple t = new Triple(s,p,o);
	   bgp.add(t);
	   vars.addAll(t.getVars());
   }
   
   public void addTriple(Triple t) {
	   bgp.add(t);
	   vars.addAll(t.getVars());
   }

   public void addChild(TreeNode node) {
	   children.add(node);
	   vars.addAll(node.getVars());
   }
   
   public Set<String> getVars() {
	   return vars;
   }
   
   public void updateVars() {
	   vars.clear();
	   for (TreeNode child : children) {
		   child.updateVars();
		   vars.addAll(child.getVars());
	   }
	   for (Triple t : bgp) {
		   vars.addAll(t.getVars());
	   }
   }

   public boolean hasChildren() {
	   return !children.isEmpty();
   }
   
   public String toString() {
	   String s = "";
	   
	   for (Triple t : bgp) {
		   s += t.toString() + " . ";
	   }
	   for (TreeNode t : children ) {
		   s += "OPTIONAL { " + t.toString() + " } . "; 
	   }
	   
	   return s.substring(0, s.length()-2);
   }
   
   public HashSet<String> getLocalVars() {
	   HashSet<String> var = new HashSet<String>();
	   
	   for (Triple t : bgp) var.addAll(t.getVars());
	   
	   return var;
   }
   
}