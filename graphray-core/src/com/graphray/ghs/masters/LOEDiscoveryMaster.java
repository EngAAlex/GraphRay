/*******************************************************************************
 * Copyright 2017 Alessio Arleo
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License.  You may obtain a copy
 * of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/
package com.graphray.ghs.masters;

import org.apache.giraph.master.MasterCompute;
import org.apache.log4j.Logger;

import com.graphray.ghs.computations.LOEDiscoveryComputation.LOEDiscoveryREPORT_DELIVERY;
import com.graphray.ghs.computations.LOEDiscoveryComputation.LOEDiscoveryREPORT_GENERATION;
import com.graphray.ghs.computations.LOEDiscoveryComputation.LOEDiscoveryTEST;
import com.graphray.ghs.computations.LOEDiscoveryComputation.LOEDiscoveryTEST_REPLY;

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
		if(counter == 0){
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
			counter = 0;
		return true;
	}

}
