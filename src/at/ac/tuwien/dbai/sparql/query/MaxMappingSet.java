package at.ac.tuwien.dbai.sparql.query;

import java.util.Iterator;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=false)
public class MaxMappingSet extends MappingSet {
	/**
	 * 
	 */
	public MaxMappingSet () {
		super();
	}
	
	private static final long serialVersionUID = 2843732909770345154L;
	
	public boolean add (Mapping m) {
		boolean isSubsumed = false;
		
		if (!this.contains(m)){
			for (Iterator<Mapping> it = iterator(); it.hasNext() && !isSubsumed; ) {
				Mapping m1 = it.next();
				if (m1.subsumes(m)) isSubsumed=true;
				if (m.subsumes(m1)) it.remove();
			}
			if (!isSubsumed) {
				super.add(m);
				return true;
			}
		}
		return false;
	}

	public void addAll(MappingSet mset) {
		for (Mapping m : mset) add(m);
	}
}
