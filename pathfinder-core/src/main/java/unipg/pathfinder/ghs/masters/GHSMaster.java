/**
 * 
 */
package unipg.pathfinder.ghs.masters;

import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.BooleanWritable;

import unipg.pathfinder.ghs.computations.EdgeConnectionRoutine;
import unipg.pathfinder.ghs.computations.LeafDiscoveryRoutine;
import unipg.pathfinder.ghs.computations.MISRoutine;
import unipg.pathfinder.masters.MSTPathfinderMasterCompute;

/**
 * @author spark
 *
 */
public class GHSMaster {

	MasterCompute master;
	LOEDiscoveryMaster lD;	
	EdgeConnectionRoutine ecr;
	LeafDiscoveryRoutine ldr;
	MISRoutine mr;
	boolean setup = true;
	int counter = 0;

	boolean firstConnection = true;

	/**
	 * 
	 */
	public GHSMaster(MasterCompute master, LOEDiscoveryMaster lD, EdgeConnectionRoutine ecr) {
		this.master = master;
		this.lD = lD;
		this.ecr = ecr;
		ldr = new LeafDiscoveryRoutine(master);
		mr = new MISRoutine(master);
	}

	public boolean compute(){
		if(counter == 0){
			if(lD.compute()){
				if(((BooleanWritable)master.getAggregatedValue(MSTPathfinderMasterCompute.cGHSProcedureCompletedAggregator)).get())
					return true;
				else{
					master.getContext().getCounter(MSTPathfinderMasterCompute.counterGroup, MSTPathfinderMasterCompute.cGHSrounds).increment(1);
					//					master.setComputation(LOEConnection.class);
					ecr.compute();
					counter++;
					return false;
				}
			}
		}else if(counter == 1){
			//			master.setComputation(LOEConnectionEncore.class);
			if(ecr.compute())
				counter++;
//			else
//				return false;			
		}
		if(counter == 2){
			counter++;			
			ldr.compute();
		}else if(counter == 3)	{
			if(ldr.compute())
				counter++;										
		}else if(counter == 4)
			if(mr.compute())
				counter = 0;
		return false;
	}
}
