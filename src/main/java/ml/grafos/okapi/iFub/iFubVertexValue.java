/**	
 * Inria Sophia Antipolis - Team COATI - 2015 
 * @author Flavian Jacquot
 * 
 */
package ml.grafos.okapi.iFub;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
/*
 * This class represent the vertex's values used in iFub
 * Hold 3 variable : layer, distance from source, was source (=we explored the graph from this vertex)
 * 
 */
public class iFubVertexValue implements Writable{
	private long layer;
	private long distance;
	private boolean wasSource=false;
	
	public iFubVertexValue() {
		this.layer = Long.MAX_VALUE;
		this.distance = Long.MAX_VALUE;
	}
	
	public iFubVertexValue(long vertexLayer) {
		this.layer = vertexLayer;
		this.distance = Long.MAX_VALUE;
	}
	@Override
	public void readFields(DataInput in) throws IOException {

		this.distance = in.readLong();
		this.layer = in.readLong();
		this.wasSource = in.readBoolean();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(layer);
		out.writeLong(distance);
		out.writeBoolean(wasSource);
	}

	@Override
	public String toString() {
		return layer +" "+ distance + "" + wasSource;
	}
	public long getDistance() {
		return distance;
	}

	public void setDistance(long distance) {
		this.distance = distance;
	}

	public long getLayer() {
		return layer;
	}

	public void setLayer(long vertexLayer) {
		this.layer = vertexLayer;
	}
	public boolean wasSource() {
		return wasSource;
	}
	public void setSource(boolean wasSource) {
		this.wasSource = wasSource;
	}
	
}
