package at.ac.tuwien.dbai.sparql.query;

import java.util.Map.Entry;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public final class Prefix {
   public static Prefix RDFS = new Prefix("rdfs","http://www.w3.org/2000/01/rdf-schema");
   public static Prefix RDF = new Prefix("rdf","http://www.w3.org/1999/02/22-rdf-syntax-ns");
   public static Prefix OWL = new Prefix("owl","http://www.w3.org/2002/07/owl");
   
   private final String prefix;
   private final String uri;
   
   public String toString() {
	   return getPrefix() + ": <" + getUri() +"#>";
   }
   
   public static String toString(Entry<String,String> e ) {
	   return e.getKey() + ": <" + e.getValue() + "#>";
   }
}
