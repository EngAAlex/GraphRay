/**
 * 
 */
package unipg.pathfinder.boruvka.masters;

import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.BooleanWritable;

import unipg.pathfinder.boruvka.computations.BoruvkaComputations.BoruvkaRootUpdatePing;
import unipg.pathfinder.boruvka.computations.BoruvkaComputations.BoruvkaRootUpdateReply;
import unipg.pathfinder.boruvka.computations.BoruvkaComputations.BoruvkaSetup;
import unipg.pathfinder.ghs.computations.LOEDiscovery;
import unipg.pathfinder.masters.MSTPathfinderMasterCompute;

/**
 * @author spark
 *
 */
public class BoruvkaMaster {

	MasterCompute master;
	LOEDiscovery lD;
	boolean setup = true;
	int counter = 0;
	
	/**
	 * 
	 */
	public BoruvkaMaster(MasterCompute master, LOEDiscovery lD) {
		this.master = master;
		this.lD = lD;
	}

	/**
	 * 
	 */
	public boolean compute() {				
		if(setup){
			master.setComputation(BoruvkaSetup.class);
			setup = false;
			return false;
		}
		
		if(counter == 3){
			if(((BooleanWritable)master.getAggregatedValue(MSTPathfinderMasterCompute.boruvkaProcedureCompletedAggregator)).get())
				return true;
			else{
				counter = 0;
			}
		}
		
		else if(counter == 0){
			if(lD.compute() != false)
				counter++;
			return false;
		}else if(counter == 1){
			master.setComputation(BoruvkaRootUpdatePing.class);
			counter++;
			return false;
		}else if(counter == 2){
			master.setComputation(BoruvkaRootUpdateReply.class);
			counter++;
			return false;
		}
		
		return true;
	}
	
}
