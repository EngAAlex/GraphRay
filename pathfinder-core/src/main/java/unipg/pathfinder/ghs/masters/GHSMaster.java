/**
 * 
 */
package unipg.pathfinder.ghs.masters;

import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.BooleanWritable;

import unipg.pathfinder.ghs.computations.LOEDiscovery;
import unipg.pathfinder.ghs.computations.LeafDiscoveryRoutine;
import unipg.pathfinder.ghs.computations.MISRoutine;
import unipg.pathfinder.ghs.computations.GHSComputations.LOEConnection;
import unipg.pathfinder.masters.MSTPathfinderMasterCompute;

/**
 * @author spark
 *
 */
public class GHSMaster {

	MasterCompute master;
	LeafDiscoveryRoutine ldr;
	MISRoutine mr;
	LOEDiscovery lD;
	boolean setup = true;
	int counter = 0;

	/**
	 * 
	 */
	public GHSMaster(MasterCompute master, LOEDiscovery lD) {
		this.master = master;
		this.lD = lD;
		ldr = new LeafDiscoveryRoutine(master);
		mr = new MISRoutine(master);
	}

	public boolean compute(){
		if(counter == 0){
			if(lD.compute())
				if(((BooleanWritable)master.getAggregatedValue(MSTPathfinderMasterCompute.cGHSProcedureCompletedAggregator)).get())
					return true;
				else counter++;			
		}
		if(counter == 1){
			master.setComputation(LOEConnection.class);
			counter++;
		}else if(counter == 2){
			if(ldr.compute())
				counter++;										
		}else if(counter == 3)
			if(mr.compute())
				counter = 0;
		return false;
	}
}


