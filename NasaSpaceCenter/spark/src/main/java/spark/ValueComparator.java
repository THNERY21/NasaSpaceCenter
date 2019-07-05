package spark;

import java.util.Map;

public class ValueComparator implements java.util.Comparator {
		private Map m = null; // the original map 

		public ValueComparator(Map m) {
			this.m = m;
		}

		public int compare(Object o1, Object o2) {
			// handle some exceptions here 
			Integer v1 = (Integer) m.get(o2);
			Integer v2 = (Integer) m.get(o1);
			// make sure the values implement Comparable
			
			return v1.compareTo(v2);
		}
		// do something similar in equals. 

	} 