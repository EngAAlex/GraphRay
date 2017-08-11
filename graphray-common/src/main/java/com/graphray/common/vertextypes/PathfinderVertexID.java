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
package com.graphray.common.vertextypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

public class PathfinderVertexID extends LongWritable {

//	int layer;
	
	/**
	 * 
	 */
	public PathfinderVertexID() {
		super();
//		layer = 0;
	}
	
	/**
	 * @param string
	 */
	public PathfinderVertexID(String string) {
		this(Long.parseLong(string));
	}
	
	public PathfinderVertexID(PathfinderVertexID id){
		set(id.get());
	}
	

	/**
	 * @param value
	 */
	public PathfinderVertexID(long value) {
		super(value);
//		layer = 0;
	}
	
	public PathfinderVertexID(long value, int layer){
		this(value);
//		this.layer = layer;
	}

//	/**
//	 * @return the layer
//	 */
//	public int getLayer() {
//		return layer;
//	}

	/**
	 * 
	 */
	public PathfinderVertexID copy() {
		return new PathfinderVertexID(get());
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.LongWritable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
//		layer = in.readInt();
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.LongWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
//		out.writeInt(layer);
	}

}
