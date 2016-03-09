package at.ac.tuwien.dbai.sparql.query;

import java.util.HashSet;
import java.util.Iterator;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=false)
public class MappingSet extends HashSet<Mapping> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3307148917499780975L;

	public MappingSet () {
		super();
	}
	
	public MappingSet (MappingSet M) {
		super();
		this.addAll(M);
	}
	
	public void extend (MappingSet M) {
		MappingSet toAdd = new MappingSet();		
		
		if (M.size() > 0) {
			for (Iterator<Mapping> it = this.iterator(); it.hasNext(); ) {
				Mapping m = it.next();
				boolean added = false;
				for (Mapping m_new : M ) {
					Mapping m_big = m.extend(m_new);
				
					if (m_big != null) {
						toAdd.add(m_big);
						added = true;
					}
				}
				if (added) it.remove();
			}
		
			this.addAll(toAdd);
		}
	}
	
	@Override
	public String toString() {
		return super.toString();
	}
}
