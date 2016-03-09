package at.ac.tuwien.dbai.opteval;

public final class Util {
    private static final String characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    public static final String DBAI_IRI = "http://dbai.tuwien.ac.at/sparql/";
	
	public static String generateString(int length)
	{
	    char[] text = new char[length];
	    for (int i = 0; i < length; i++)
	    {
	        text[i] = characters.charAt((int) (Math.random()*characters.length()));
	    }
	    return new String(text);
	}
	
	/***
	 * 
	 * Outputs an error message
	 * 
	 * @param message The output error message
	 * @param fatal If true, procedure exits current process
	 */
	public static void error(String message, boolean fatal) {
		
		if (fatal) {
			System.err.println(message);
			System.exit(1);
		} else
			System.err.println(message);
	}
	
	/***
	 * 
	 * Outputs a fatal error and exits the current program
	 * 
	 * @param message The output error message
	 */
	public static void error(String message) { error(message,true); }
}
