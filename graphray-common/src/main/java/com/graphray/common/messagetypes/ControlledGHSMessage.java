/**
 * 
 */
package com.graphray.common.messagetypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;

import com.graphray.common.vertextypes.PathfinderVertexID;

/**
 * @author spark
 *
 */
public class ControlledGHSMessage extends DoubleWritable{
	
	PathfinderVertexID senderID;
	PathfinderVertexID fragmentID;
	short status;
	
	public static final short ROOT_STATUS = 21;
	public static final short ROOT_UPDATE = 20;
	
	public static final short LEAF_DISCOVERY = 10;
	public static final short DEPTH_1_DISCOVERY = 11;
	public static final short MIS_MESSAGE = 12;
	
	public static final short FORCE_ACCEPT = 6;
	public static final short LOEs_DEPLETED = 5;
	public static final short CONNECT_AS_BRANCH = 42;	
	public static final short CONNECT_REPLY = 41;		
	public static final short CONNECT_TEST = 40;	
	public static final short CONNECT_FROM_ROOT_MESSAGE = 400;	
	public static final short CONNECT_MESSAGE = 4;	
	public static final short REPORT_MESSAGE = 3;
	public static final short ACCEPT_MESSAGE = 2;
	public static final short TEST_MESSAGE = 1;
	public static final short REFUSE_MESSAGE = 0;

	/**
	 * 
	 */
	public ControlledGHSMessage() {
		super();
		senderID = new PathfinderVertexID();
		fragmentID = new PathfinderVertexID();
	}
	
	public ControlledGHSMessage(PathfinderVertexID senderID, short status){
		this();
		this.senderID = senderID;
		fragmentID = new PathfinderVertexID();		
		this.status = status;
	}
	
	public ControlledGHSMessage(PathfinderVertexID senderID, double value, short status){
		super(value);		
		this.senderID = senderID;
		fragmentID = new PathfinderVertexID();
		this.status = status;
	}

	public ControlledGHSMessage(PathfinderVertexID senderID, PathfinderVertexID fragmentID, short status){
		this();
		this.senderID = senderID;
		this.status = status;
		this.fragmentID = fragmentID;
	}

	public ControlledGHSMessage(PathfinderVertexID senderID, PathfinderVertexID fragmentID, double  value, short status){
		super(value);
		this.senderID = senderID;
		this.status = status;
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
	 * @return the status
	 */
	public short getStatus() {
		return status;
	}

//	/**
//	 * @return the startingDepth
//	 */
//	public int getDepth() {
//		return startingDepth;
//	}
//
//	/**
//	 * @param startingDepth the startingDepth to set
//	 */
//	public void setDepth(int startingDepth) {
//		this.startingDepth = startingDepth;
//	}

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
		super.readFields(in);
		senderID.readFields(in);
		fragmentID.readFields(in);
		status = in.readShort();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		super.write(out);
		senderID.write(out);
		fragmentID.write(out);
		out.writeShort(status);
	}

	/**
	 * @return
	 */
	public ControlledGHSMessage copy() {
		return new ControlledGHSMessage(senderID.copy(), fragmentID.copy(), get(), status);
	}

}
