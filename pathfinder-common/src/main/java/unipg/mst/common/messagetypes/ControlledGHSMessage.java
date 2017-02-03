/**
 * 
 */
package unipg.mst.common.messagetypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

/**
 * @author spark
 *
 */
public class ControlledGHSMessage extends DoubleWritable {
	
	long senderID;
	long fragmentID;
	short status;
	int startingDepth;
	
	public static final short CONNECT_MESSAGE = 3;	
	public static final short REPORT_MESSAGE = 3;
	public static final short ACCEPT_MESSAGE = 2;
	public static final short TEST_MESSAGE = 1;
	public static final short REFUSE_MESSAGE = 0;

	/**
	 * 
	 */
	public ControlledGHSMessage() {
		super();
	}
	
	public ControlledGHSMessage(long senderID, short status){
		this();
		this.senderID = senderID;
		this.status = status;
	}
	
	public ControlledGHSMessage(long senderID, double value, short status){
		super(value);
		this.senderID = senderID;
		this.status = status;
	}

	public ControlledGHSMessage(long senderID, long fragmentID, short status){
		this();
		this.senderID = senderID;
		this.status = status;
		this.fragmentID = fragmentID;
	}

	public ControlledGHSMessage(long senderID, long fragmentID, double  value, short status){
		super(value);
		this.senderID = senderID;
		this.status = status;
		this.fragmentID = fragmentID;
	}	
	
	public ControlledGHSMessage(long senderID,  long fragmentID, int startingDepth, short status){
		this(senderID, fragmentID, status);
		this.startingDepth = startingDepth;
	}
	
		/**
	 * @return the senderID
	 */
	public long getSenderID() {
		return senderID;
	}

	/**
	 * @return the status
	 */
	public short getStatus() {
		return status;
	}

	/**
	 * @return the startingDepth
	 */
	public int getDepth() {
		return startingDepth;
	}

	/**
	 * @param startingDepth the startingDepth to set
	 */
	public void setDepth(int startingDepth) {
		this.startingDepth = startingDepth;
	}

	/**
	 * @return the fragmentID
	 */
	public long getFragmentID() {
		return fragmentID;
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#readFields(java.io.DataInput)
	 */
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		senderID = in.readLong();
		fragmentID = in.readLong();
		status = in.readShort();
		startingDepth = in.readInt();
	}

	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.Writable#write(java.io.DataOutput)
	 */
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeLong(senderID);
		out.writeLong(fragmentID);
		out.writeShort(status);
		out.writeInt(startingDepth);
	}

}
