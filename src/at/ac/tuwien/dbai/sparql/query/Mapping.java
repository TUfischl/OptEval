package at.ac.tuwien.dbai.sparql.query;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import lombok.Data;

@Data
public class Mapping {
   private Map<String,String> mapping;
   
   public Mapping() {
	   mapping = new HashMap<String,String>();
   }
   
   public Mapping(Mapping m2) {
	   mapping = new HashMap<String,String>(m2.mapping);
   }

public void add(String var, String value) { mapping.put(var,  value); }
   
   public String get(String var) { return mapping.get(var); }
   
   public boolean subsumes (Mapping m) {
	   if (mapping.size() >= m.mapping.size()) {
		  Set<String> keys = m.mapping.keySet();
	      if (mapping.keySet().containsAll(keys)) {
	    	  for (String key : keys) {
	    		  String value = m.mapping.get(key);
	    		  if (!mapping.get(key).equals(value)) {
	    			  if (!(value == null))
	    				  return false;
	    		  }
	    	  }
	      } else return false;
	   } else return false;
	   return true;
   }
   
   public Set<String> domain() {
	   return new HashSet<String>(mapping.keySet());
   }
   
   public void project(Set<String> vars) {
	   for (Iterator<String> it = mapping.keySet().iterator(); it.hasNext() ; ) {
		   if (!vars.contains(it.next()))
			   it.remove();
	   }
   }

   public boolean isEmpty() {
	  return mapping.isEmpty();
   }

   public void remove(String var) {
      mapping.remove(var);
   }

   public boolean compatible(Mapping m_new) {
	  Set<String> inter = domain();
	  inter.retainAll(m_new.domain());
	  
	  for (String key : inter) {
		  if (!mapping.get(key).equals(m_new.get(key)))
			  return false;
	  }
	  return true;
   }

   public Mapping extend(Mapping m_new) {
	  if (compatible(m_new)) {
		  Mapping m = new Mapping(this);
		  m.mapping.putAll(m_new.mapping);
		  return m;
	  }
	  return null;
   }

	public Map<String, String> getMap() {
		return mapping;
	}
}