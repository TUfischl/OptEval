package at.ac.tuwien.dbai.sparql.query;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Collectors;

public class TreeFactory {
	
	private static int PREFIX_STATE = 0;
	private static int SELECT_STATE = 1;
	private static int WHERE_STATE = 2;
	
	private static int i;
	private static String[] words;
	
	public static PatternTree create(File file) {
		try {
			return create(Files.lines(file.toPath())
			                   .parallel() // for parallel processing   
			                   //.filter(line -> line.length() > 2) // to filter some lines by a predicate
			                   .map(String::trim) // to change line 
			                   .collect(Collectors.joining("\n")));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // to join lines);
		return null;
	}
	
    public static PatternTree create (String query) throws ParseException {
    	PatternTree pt = new PatternTree();
    	int state = PREFIX_STATE;
    	
    	// Removes line breaks from query and breaks the query into words
    	words = query.replaceAll("\\r\\n|\\r|\\n", " ").replaceAll("\\s+", " ").split("\\s");
    		
    	for (i = 0; i<words.length; i++) {
    		switch (words[i].toUpperCase()) {
    			case "PREFIX":
    				if (state <= PREFIX_STATE) {
    					if (words[i+1].endsWith(":")) 
    						if (words[i+2].startsWith("<") && words[i+2].endsWith(">"))
    							pt.addPrefix(words[++i].replace(':', ' ').trim(),words[++i].replace('<', ' ').replace('>',' ').replace('#',' ').trim());
    						else throw new ParseException("Lexical error in PREFIX statement!");
    					else throw new ParseException("Lexical error in PREFIX statement!");
    				}
    				else throw new ParseException("No PREFIX statements after the SELECT keyword!");
    				break;
    			case "SELECT":
    				if (state < SELECT_STATE) {
    					state = SELECT_STATE;
    					if (words[i+1] == "*")
    						pt.addProjVars("*");
    					else 
    					    while (words[i+1].startsWith("?"))
    						    pt.addProjVars(words[++i]);
    				}
    				else throw new ParseException("No further SELECT statements after the SELECT keyword!");
    		        break;
    			case "WHERE":
    				if (state < WHERE_STATE) {
    					state = WHERE_STATE;
    				    if (words[i+1].equals("{")) {
    				    	i++;
    				    	pt.setRoot(createNode());
    				    } else throw new ParseException("WHERE statement must start with '{'");
    				}
    				else throw new ParseException("No further WHERE statements after the WHERE keyword!");
    		        break;
    		    default: throw new ParseException("Not a valid Query!");
    		}
    	}
    	
    	return pt;
    }

	private static TreeNode createNode() throws ParseException {
		TreeNode n = new TreeNode();
		
		do {
		   if (validURI(words[i+1]))
              if (validURI(words[i+2]))
            	  if (!words[i+2].startsWith("?"))
            		  if (validURI(words[i+3]))
            			  n.addTriple(words[++i],words[++i],words[++i]);
            		  else
            			  throw new ParseException("Not a valid URI: " + words[i+3]);
            	  else
            		  throw new ParseException("No variables at predicate position: " + words[i+2]);
              else
        		  throw new ParseException("Not a valid URI: " + words[i+2]);
		   else
     		  throw new ParseException("Not a valid URI: " + words[i+1]);
		   
		   if (words[i+1].equals(".")) i++; 
		   else if (words[i+1].equals("}")) { i++; return n; } 
		   else throw new ParseException("Triple Pattern must end with '.' or '}'!");
		   
		   while (i < words.length - 2 && words[i+1].equals("OPTIONAL") && words[i+2].equals("{")) {
			   i += 2;
			   n.addChild(createNode());
		   }
		} while (i < words.length && !words[i].equals("}") && i < words.length - 1 && !words[i+1].equals("}"));
		i++;
		if (i < words.length && words[i].equals("}")) i++;
		return n;
	}

	private static boolean validURI(String string) {
		switch (string) {
		case ".":
		case "OPTIONAL":
		case "}":
		case "{":
			return false;
		}
		return true;
	}

	
}
