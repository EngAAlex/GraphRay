/**
 * 
 */
package unipg.pathfinder.boruvka.masters;

import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.IntWritable;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.pathfinder.boruvka.computations.BoruvkaComputations.BoruvkaRootUpdatePing;
import unipg.pathfinder.boruvka.computations.BoruvkaComputations.BoruvkaRootUpdateReply;
import unipg.pathfinder.boruvka.computations.BoruvkaComputations.BoruvkaSetup;
import unipg.pathfinder.ghs.computations.GHSComputations.LOEConnection;
import unipg.pathfinder.ghs.computations.GHSComputations.LOEConnectionEncore;
import unipg.pathfinder.ghs.masters.LOEDiscoveryMaster;
import unipg.pathfinder.masters.MSTPathfinderMasterCompute;

/**
 * @author spark
 *
 */
public class BoruvkaMaster {

	MasterCompute master;
	LOEDiscoveryMaster lD;
	boolean setup = true;
	int counter;

	/**
	 * 
	 */
	public BoruvkaMaster(MasterCompute master, LOEDiscoveryMaster lD) {
		this.master = master;
		this.lD = lD;
		counter = 0;
	}

	/**
	 * 
	 */
	public boolean compute() {				
		if(setup){
			master.setComputation(BoruvkaSetup.class);
			setup = false;
			master.setAggregatedValue(MSTPathfinderMasterCompute.loesToDiscoverAggregator, new IntWritable(PathfinderEdgeType.INTERFRAGMENT_EDGE));
			return false;
		}

		if(counter == 0){
			if(lD.compute()){
				counter++;
				master.setComputation(LOEConnection.class);
			}
			return false;
		}else if(counter == 1 || counter == 2){
			master.setComputation(LOEConnectionEncore.class);
			counter++;
			return false;
		}else if(counter == 3){
			if(((IntWritable)master.getAggregatedValue(MSTPathfinderMasterCompute.boruvkaProcedureCompletedAggregator)).get() == 1)
				return true;
			else
				master.setAggregatedValue(MSTPathfinderMasterCompute.boruvkaProcedureCompletedAggregator, new IntWritable(0));
			master.setComputation(BoruvkaRootUpdatePing.class);
			counter++;
			return false;
		}else if(counter == 4){
			master.setComputation(BoruvkaRootUpdateReply.class);
			counter = 0;
			return false;
		}

		return true;
	}

}
