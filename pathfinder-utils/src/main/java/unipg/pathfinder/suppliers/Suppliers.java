/**
 * 
 */
package unipg.pathfinder.suppliers;

import org.apache.giraph.function.Supplier;

/**
 * @author spark
 *
 */
public class Suppliers {

	public static class BoruvkaSupplier implements Supplier<Boolean>{
		
		/**
		 * 
		 */
		private static final long serialVersionUID = -6425009584086990301L;
		/**
		 * 
		 */
		int fragments;		
		
		/**
		 * 
		 */
		public BoruvkaSupplier() {
			fragments = 0;
		}
		
		public void updateFragments(int fragments){
			this.fragments = fragments;
		}

		/* (non-Javadoc)
		 * @see org.apache.giraph.function.Supplier#get()
		 */
		public Boolean get() {
			return fragments == 1;
		}

				
	}
	
	public static class CGHSSupplier implements Supplier<Boolean>{
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 329192886630032629L;
		/**
		 * 
		 */
		boolean cOntinue = true;
		
		/**
		 * 
		 */
		public CGHSSupplier() {
		}
		
		public void update(boolean cOntinue){
			this.cOntinue = this.cOntinue && cOntinue;
		}

		/* (non-Javadoc)
		 * @see org.apache.giraph.function.Supplier#get()
		 */
		public Boolean get() {
			return cOntinue;
		}				
	}
}
