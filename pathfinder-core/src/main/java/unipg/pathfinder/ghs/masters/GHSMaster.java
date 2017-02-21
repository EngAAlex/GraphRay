/**
 * 
 */
package unipg.pathfinder.ghs.masters;

import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.BooleanWritable;

import unipg.pathfinder.ghs.computations.LeafDiscoveryRoutine;
import unipg.pathfinder.ghs.computations.MISRoutine;
import unipg.pathfinder.ghs.computations.GHSComputations.LOEConnection;
import unipg.pathfinder.ghs.computations.GHSComputations.LOEConnectionEncore;
import unipg.pathfinder.masters.MSTPathfinderMasterCompute;

/**
 * @author spark
 *
 */
public class GHSMaster {

	MasterCompute master;
	LeafDiscoveryRoutine ldr;
	MISRoutine mr;
	LOEDiscoveryMaster lD;
	boolean setup = true;
	int counter = 0;

	boolean firstConnection = true;

	/**
	 * 
	 */
	public GHSMaster(MasterCompute master, LOEDiscoveryMaster lD) {
		this.master = master;
		this.lD = lD;
		ldr = new LeafDiscoveryRoutine(master);
		mr = new MISRoutine(master);
	}

	public boolean compute(){
		if(counter == 0){
			if(lD.compute()){
				if(((BooleanWritable)master.getAggregatedValue(MSTPathfinderMasterCompute.cGHSProcedureCompletedAggregator)).get())
					return true;
				else{
					master.setComputation(LOEConnection.class);
					counter++;
					return false;
				}
			}
		}else if(counter == 1){
			master.setComputation(LOEConnectionEncore.class);
			counter++;
			return false;			
		}else if(counter == 2){
//			if(firstConnection){
//				firstConnection = false;
//				mr.compute();
//				counter = 4;
//			}else{
				counter++;			
//				if(((BooleanWritable)master.getAggregatedValue(MSTPathfinderMasterCompute.messagesLeftAggregator)).get())
					ldr.compute();
//			}
		}else if(counter == 3)	{
			if(ldr.compute())
				counter++;										
		}else if(counter == 4)
			if(mr.compute())
				counter = 0;
		return false;
	}
}


