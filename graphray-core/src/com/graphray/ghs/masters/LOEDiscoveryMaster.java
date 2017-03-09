/**
 * 
 */
package com.graphray.ghs.masters;

import org.apache.giraph.master.MasterCompute;
import org.apache.log4j.Logger;

import com.graphray.ghs.computations.LOEDiscoveryComputation.LOEDiscoveryCleanup;
import com.graphray.ghs.computations.LOEDiscoveryComputation.LOEDiscoveryREPORT_DELIVERY;
import com.graphray.ghs.computations.LOEDiscoveryComputation.LOEDiscoveryREPORT_GENERATION;
import com.graphray.ghs.computations.LOEDiscoveryComputation.LOEDiscoveryTEST;
import com.graphray.ghs.computations.LOEDiscoveryComputation.LOEDiscoveryTEST_REPLY;

/**
 * @author spark
 *
 */
public class LOEDiscoveryMaster {

	MasterCompute master;
	int counter = 0;

	/**
	 * 
	 */
	public LOEDiscoveryMaster(MasterCompute master) {
		this.master = master;
	}
	
	public boolean compute(){
		if(master.getSuperstep() == 0){
			return false;
		}
		if(counter == -1){
			master.setComputation(LOEDiscoveryCleanup.class);
			counter++;
			return false;
		}else if(counter == 0){
			counter++;
			master.setComputation(LOEDiscoveryTEST.class);
			return false;
		}else if(counter == 1){
			master.setComputation(LOEDiscoveryTEST_REPLY.class);
			counter++;
			return false;
		}else if(counter == 2){
			master.setComputation(LOEDiscoveryREPORT_GENERATION.class);
			counter++;
			return false;
		}else if(counter == 3){
			master.setComputation(LOEDiscoveryREPORT_DELIVERY.class);
			counter++;
			return false;
		}else if(counter == 4)
			counter = -1;
		return true;
	}

}
