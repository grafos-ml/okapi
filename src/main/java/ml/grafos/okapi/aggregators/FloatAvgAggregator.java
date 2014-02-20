package ml.grafos.okapi.aggregators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import ml.grafos.okapi.aggregators.FloatAvgAggregator.PartialAvg;

import org.apache.giraph.aggregators.BasicAggregator;
import org.apache.hadoop.io.Writable;

public class FloatAvgAggregator extends BasicAggregator<PartialAvg> {

  @Override
  public void aggregate(PartialAvg value) {
    getAggregatedValue().combine(value);
  }

  @Override
  public PartialAvg createInitialValue() {
    return new PartialAvg(0,0);
  }

  public static class PartialAvg implements Writable {
    public float partialSum;
    public int partialCount;

    public PartialAvg() {
      partialSum = 0;
      partialCount = 0;
    }

    public PartialAvg(float sum, int count) {
      partialSum = sum;
      partialCount = count;
    }

    public void combine(PartialAvg other) {
      this.partialSum += other.partialSum;
      this.partialCount += other.partialCount;
    }

    public float get() {
      return partialSum/(float)partialCount;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
      partialSum = input.readFloat();
      partialCount = input.readInt();
    }

    @Override
    public void write(DataOutput output) throws IOException {
      output.writeFloat(partialSum);
      output.writeInt(partialCount);
    }
    
    @Override
    public String toString() {
      return partialCount+" "+partialSum+" "+get();
    }
  }
}
