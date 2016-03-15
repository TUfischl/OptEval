package at.ac.tuwien.dbai.sparql.query;

import java.util.Set;

public interface Query {
	public static int QUERY_CQ = 0;
	public static int QUERY_PT = 1;
	public static int QUERY_HolRewPT = 2;
	public static int QUERY_ModRewPT = 3;
	
    public abstract String getQuery();
    public abstract int getType();
    public abstract Set<String> getVars();
    public String getDefaultPrefix();
	public abstract int count();
}
