package at.ac.tuwien.dbai.sparql.query;

public class ParseException extends Exception {
    private String msg;
	
	public ParseException(String msg) {
    	this.msg = msg;
    }
	
	public String getMessage() {
		return msg;
	}
	
}
