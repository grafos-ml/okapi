package ml.grafos.okapi.graphs.maxbmatching;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import ml.grafos.okapi.graphs.maxbmatching.MBMEdgeState.State;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

public class MBMMessage implements Writable {
    private LongWritable id;
    private State state;

    public LongWritable getId() {
        return id;
    }

    public void setId(LongWritable id) {
        this.id = id;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    public MBMMessage(LongWritable id, State proposed) {
        this.id = id;
        this.state = proposed;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        id = new LongWritable(in.readLong());
        state = State.fromValue(in.readByte());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id.get());
        out.writeByte(state.value());
    }
}
