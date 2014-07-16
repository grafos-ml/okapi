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

    public MBMEdgeState(double weight) {
        super();
        this.setWeight(weight);
    }

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
        return weight + "\t" + state.toString();
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((state == null) ? 0 : state.hashCode());
        long temp;
        temp = Double.doubleToLongBits(weight);
        result = prime * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof MBMEdgeState))
            return false;
        MBMEdgeState other = (MBMEdgeState) obj;
        if (state != other.state)
            return false;
        if (Double.doubleToLongBits(weight) != Double.doubleToLongBits(other.weight))
            return false;
        return true;
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
        DEFAULT ((byte) 1), // starting state
        PROPOSED((byte) 2), // proposed for inclusion in the matching
        REMOVED ((byte) 3), // cannot be included in the matching
        INCLUDED((byte) 4); // included in the matching

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
