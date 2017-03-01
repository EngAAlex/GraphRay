/**
 * 
 */
package unipg.pathfinder.boruvka.masters;

import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import unipg.mst.common.edgetypes.PathfinderEdgeType;
import unipg.pathfinder.boruvka.computations.BoruvkaComputations.BoruvkaPathfinderUpdatePing;
import unipg.pathfinder.boruvka.computations.BoruvkaComputations.BoruvkaPathfinderUpdateReply;
import unipg.pathfinder.boruvka.computations.BoruvkaComputations.BoruvkaRootUpdateCompletion;
import unipg.pathfinder.boruvka.computations.BoruvkaComputations.BoruvkaRootUpdateConfirmationCompletion;
import unipg.pathfinder.boruvka.computations.BoruvkaComputations.BoruvkaRootUpdateConfirmationPing;
import unipg.pathfinder.boruvka.computations.BoruvkaComputations.BoruvkaRootUpdateConfirmationReply;
import unipg.pathfinder.boruvka.computations.BoruvkaComputations.BoruvkaRootUpdateSetup;
import unipg.pathfinder.boruvka.computations.BoruvkaComputations.BoruvkaSetup;
import unipg.pathfinder.ghs.computations.EdgeConnectionRoutine;
import unipg.pathfinder.ghs.masters.LOEDiscoveryMaster;
import unipg.pathfinder.masters.MSTPathfinderMasterCompute;

/**
 * @author spark
 *
 */
public class BoruvkaMaster {
	
	protected static Logger log = Logger.getLogger(BoruvkaMaster.class);

	MasterCompute master;
	LOEDiscoveryMaster lD;
	EdgeConnectionRoutine ecr;
	boolean setup = true;
	int counter;

	/**
	 * 
	 */
	public BoruvkaMaster(MasterCompute master, LOEDiscoveryMaster lD, EdgeConnectionRoutine ecr) {
		this.master = master;
		this.lD = lD;
		this.ecr = ecr;
		counter = 0;
	}

	/**
	 * 
	 */
	public boolean compute() {				
		if(setup){
			if(master.getComputation().equals(BoruvkaSetup.class)){
				master.setComputation(BoruvkaPathfinderUpdatePing.class);
				return false;
			}else if(master.getComputation().equals(BoruvkaPathfinderUpdatePing.class)){
				master.setComputation(BoruvkaPathfinderUpdateReply.class);
				setup = false;
				return false;
			}
			master.setComputation(BoruvkaSetup.class);
			master.setAggregatedValue(MSTPathfinderMasterCompute.loesToDiscoverAggregator, new IntWritable(PathfinderEdgeType.INTERFRAGMENT_EDGE));
//			setup = false;

			return false;
		}

		if(counter == 0){
			if(lD.compute()){
				counter++;
//				master.setComputation(LOEConnection.class);
				ecr.compute();
			}
			return false;
		}else if(counter == 1){
//		}else if(counter == 1 || counter == 2){
//			master.setComputation(LOEConnectionEncore.class);
			if(ecr.compute())
				counter++;
			else
				return false;
		}
		
		if(counter == 2){
			master.setComputation(BoruvkaRootUpdateSetup.class);
			counter++;
			return false;
		}else if(counter == 3){
			if(((IntWritable)master.getAggregatedValue(MSTPathfinderMasterCompute.boruvkaProcedureCompletedAggregator)).get() == 1)
				return true;
			else{
				log.info("remaining fragments: " + ((IntWritable)master.getAggregatedValue(MSTPathfinderMasterCompute.boruvkaProcedureCompletedAggregator)).get());
				master.setAggregatedValue(MSTPathfinderMasterCompute.boruvkaProcedureCompletedAggregator, new IntWritable(0));
			}
			master.getContext().getCounter(MSTPathfinderMasterCompute.counterGroup, MSTPathfinderMasterCompute.boruvkaRounds).increment(1);
			master.setComputation(BoruvkaRootUpdateConfirmationPing.class);
			counter++;
			return false;
		}else if(counter == 4){
			master.setComputation(BoruvkaRootUpdateConfirmationReply.class);
			counter++;
			return false;
		}else if(counter == 5){
			master.setComputation(BoruvkaRootUpdateConfirmationCompletion.class);
			counter++;
			return false;
		}else if(counter == 6){
			master.setComputation(BoruvkaRootUpdateCompletion.class);
			counter = 0;
			return false;
		}

		return true;
	}

}
