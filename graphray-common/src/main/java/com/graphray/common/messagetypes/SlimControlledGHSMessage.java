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
/**
 * 
 */
package com.graphray.common.messagetypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

import com.graphray.common.vertextypes.PathfinderVertexID;

public class SlimControlledGHSMessage implements Writable{

	PathfinderVertexID senderID;
	PathfinderVertexID fragmentID;

	/**
	 * 
	 */
	public SlimControlledGHSMessage() {

	}


	public SlimControlledGHSMessage(PathfinderVertexID senderID, PathfinderVertexID fragmentID){
		this.senderID = senderID;
		this.fragmentID = fragmentID;
	}



	//	public ControlledGHSMessage(long senderID,  long fragmentID, int startingDepth, short status){
	//		this(senderID, fragmentID, status);
	//		this.startingDepth = startingDepth;
	//	}

	/**
	 * @return the senderID
	 */
	public PathfinderVertexID getSenderID() {
		return senderID;
	}


	/**
	 * @return the fragmentID
	 */
	public PathfinderVertexID getFragmentID() {
		return fragmentID;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
		senderID = new PathfinderVertexID();
		fragmentID = new PathfinderVertexID();
		
		senderID.readFields(in);
		fragmentID.readFields(in);
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		senderID.write(out);
		fragmentID.write(out);
	}

	/**
	 * @return
	 */
	public SlimControlledGHSMessage copy() {
		return new SlimControlledGHSMessage(senderID.copy(), fragmentID.copy());
	}


}
