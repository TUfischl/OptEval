package at.ac.tuwien.dbai.sparql.query;

import java.util.HashSet;
import java.util.Set;

import lombok.AllArgsConstructor;
import lombok.Data;

@AllArgsConstructor
@Data
public class Triple {
   private String subject;
   private String predicate;
   private String object;
   
   public Triple(Triple t) {
      subject = t.subject;
      predicate = t.predicate;
      object = t.object; 
   }
   
   public boolean subjectIsVar () { return subject.startsWith("?"); }
   public boolean predicateIsVar () { return predicate.startsWith("?"); }
   public boolean objectIsVar () { return object.startsWith("?"); }
   
   public String toString() { return subject + " " + predicate + " " + object; }
   
   public Set<String> getVars() {
	   Set<String> vars = new HashSet<String>();
	   if (subjectIsVar()) vars.add(subject);
	   if (predicateIsVar()) vars.add(predicate);
	   if (objectIsVar()) vars.add(object);
	   return vars;
   }

public boolean containsVar(String v) {
    if (subject.equals(v) || object.equals(v) || predicate.equals(v))
	   return true;
    return false;
}

} 
