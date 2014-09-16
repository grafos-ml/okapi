package ml.grafos.okapi.graphs.maxbmatching;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import ml.grafos.okapi.graphs.maxbmatching.MBMEdgeValue.State;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class MBMMessage implements Writable {
    private LongWritable vertexID;
    private State state;

    public MBMMessage() {
    }

    public MBMMessage(LongWritable id, State proposed) {
        this.vertexID = id;
        this.state = proposed;
    }

    public LongWritable getId() {
        return vertexID;
    }

    public void setId(LongWritable id) {
        this.vertexID = id;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    @Override
    public String toString() {
        return "(" + vertexID + ", " + state + ")";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((state == null) ? 0 : state.hashCode());
        result = prime * result + ((vertexID == null) ? 0 : vertexID.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof MBMMessage))
            return false;
        MBMMessage other = (MBMMessage) obj;
        if (state != other.state)
            return false;
        if (vertexID == null) {
            if (other.vertexID != null)
                return false;
        } else if (!vertexID.equals(other.vertexID))
            return false;
        return true;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        vertexID = new LongWritable(in.readLong());
        state = State.fromValue(in.readByte());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(vertexID.get());
        out.writeByte(state.value());
    }
}
