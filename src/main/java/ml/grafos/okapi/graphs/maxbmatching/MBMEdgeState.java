package ml.grafos.okapi.graphs.maxbmatching;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Writable;

public class MBMEdgeState implements Writable {
    private double weight = 0.0;
    private State state = State.DEFAULT;

    public double getWeight() {
        return weight;
    }

    public void setWeight(double weight) {
        this.weight = weight;
    }

    public State getState() {
        return state;
    }

    public void setState(State state) {
        this.state = state;
    }

    @Override
    public String toString() {
        return state.toString() + "(" + weight + ")";
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        weight = in.readDouble();
        state = State.fromValue(in.readByte());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(weight);
        out.writeByte(state.value());
    }

    public static enum State {
        DEFAULT((byte) 1), PROPOSED((byte) 2), REMOVED((byte) 3), INCLUDED((byte) 4);

        private final byte value;
        private static final Map<Byte, State> lookup = new HashMap<Byte, State>();
        static {
            for (State s : values())
                lookup.put(s.value, s);
        }

        State(byte value) {
            this.value = value;
        }

        public static State fromValue(byte value) {
            State result = lookup.get(value);
            if (result == null)
                throw new IllegalArgumentException("Cannot build edge State from illegal value: " + value);
            return result;
        }

        public byte value() {
            return value;
        }
    }
}
