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
	
	public static class PathfinderVertexIDWithShortValue extends PathfinderVertexID {
		
		short value;
		
		public PathfinderVertexIDWithShortValue(){
			super();
		}
		
		public PathfinderVertexIDWithShortValue(PathfinderVertexID id, short value){
			super(id);
			this.value = value;
		}
		
//		/**
//		 * @return the value
//		 */
//		public short getByteValue() {
//			return value;
//		}
//
//		/**
//		 * @param value the value to set
//		 */
//		public void setByteValue(short value) {
//			this.value = value;
//		}

		/**
		 * @return the depth
		 */
		public short getDepth() {
			return value;
		}

		/**
		 * @param depth the depth to set
		 */
		public void setDepth(short depth) {
			this.value = depth;
		}

		/* (non-Javadoc)
		 * @see unipg.mst.common.vertextypes.PathfinderVertexID#readFields(java.io.DataInput)
		 */
		@Override
		public void readFields(DataInput in) throws IOException {
			super.readFields(in);
			value = in.readShort();
//			depth = in.readShort();
		}
		
		/* (non-Javadoc)
		 * @see unipg.mst.common.vertextypes.PathfinderVertexID#write(java.io.DataOutput)
		 */
		@Override
		public void write(DataOutput out) throws IOException {
			super.write(out);
			out.writeShort(value);
//			out.writeShort(depth);
		}	
	}	

}
