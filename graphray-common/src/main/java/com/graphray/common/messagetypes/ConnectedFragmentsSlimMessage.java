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
import java.util.Collection;
import java.util.HashSet;
import java.util.Stack;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import com.graphray.common.vertextypes.PathfinderVertexID;

public class ConnectedFragmentsSlimMessage extends DoubleWritable {

	protected PathfinderVertexID senderID;
	protected Stack<PathfinderVertexID> setOfFragments;

	/**
	 * 
	 */
	public ConnectedFragmentsSlimMessage() {
		super();
		senderID = new PathfinderVertexID();
//		setOfFragments = new Stack<PathfinderVertexID>();
	}


	public ConnectedFragmentsSlimMessage(PathfinderVertexID senderID, double value){
		super(value);
		this.senderID = senderID;
//		setOfFragments = new Stack<PathfinderVertexID>();
	}

	public ConnectedFragmentsSlimMessage(PathfinderVertexID senderID, double value, Collection<PathfinderVertexID> toSend){
		this(senderID, value);
		setSetOfFragments(toSend);
	}

	/**
	 * @return the senderID
	 */
	public PathfinderVertexID getSenderID() {
		return senderID;
	}


	/**
	 * @param senderID the senderID to set
	 */
	public void setSenderID(PathfinderVertexID senderID) {
		this.senderID = senderID;
	}


	/**
	 * 
	 */
	public void addToFragmentSet(PathfinderVertexID id) {
		setOfFragments.add(id);
	}		

	/**
	 * @return the setOfFragments
	 */
	public Collection<PathfinderVertexID> getSetOfFragments() {
		return setOfFragments;
	}

	/**
	 * @param setOfFragments the setOfFragments to set
	 */
	public void setSetOfFragments(Collection<PathfinderVertexID> setOfFragments) {
		//			this.setOfFragments = setOfFragments;
		if(this.setOfFragments == null)
			this.setOfFragments = new Stack<PathfinderVertexID>();
		this.setOfFragments.clear();
		for(PathfinderVertexID current : setOfFragments)
			if(!this.setOfFragments.contains(current))
				this.setOfFragments.add(current.copy());
		//			this.setOfFragments.addAll(setOfFragments);
	}

	/* (non-Javadoc)
	 * @see com.graphray.common.messagetypes.ControlledGHSMessage#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		senderID.readFields(in);
		int size = in.readInt();
		setOfFragments = new Stack<PathfinderVertexID>();
		if(size == 0)
			return;
		else
			for(int i=0; i<size; i++){
				PathfinderVertexID current = new PathfinderVertexID();
				current.readFields(in);
				setOfFragments.add(i, current.copy());
			}
		//setOfFragments.readFields(in);
	}

	/* (non-Javadoc)
	 * @see com.graphray.common.messagetypes.ControlledGHSMessage#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		senderID.write(out);
		out.writeInt(setOfFragments.size());
		if(setOfFragments.size() > 0){
			for(PathfinderVertexID current : setOfFragments){
				current.write(out);
			}
		}

		//			setOfFragments.write(out);
	}


	/**
	 * @return
	 */
	public ConnectedFragmentsSlimMessage copy() {
		return new ConnectedFragmentsSlimMessage(senderID.copy(), get(), setOfFragments);
	}


}
