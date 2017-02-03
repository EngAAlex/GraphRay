/**
 * 
 */
package unipg.mst.common.vertextypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;

/**
 * @author spark
 *
 */
public class PathfinderVertexID extends LongWritable {

	int layer;
	
	/**
	 * 
	 */
	public PathfinderVertexID() {
		super();
		layer = 0;
	}
	

	/**
	 * @param value
	 */
	public PathfinderVertexID(long value) {
		super(value);
		layer = 0;
	}
	
	public PathfinderVertexID(long value, int layer){
		this(value);
		this.layer = layer;
	}

	/**
	 * @return the layer
	 */
	public int getLayer() {
		return layer;
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.LongWritable#readFields(java.io.DataInput)
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		super.readFields(in);
		layer = in.readInt();
	}
	
	/* (non-Javadoc)
	 * @see org.apache.hadoop.io.LongWritable#write(java.io.DataOutput)
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		super.write(out);
		out.writeInt(layer);
	}

}
