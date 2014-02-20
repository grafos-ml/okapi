package ml.grafos.okapi.aggregators;

import com.google.common.base.Charsets;

import java.io.IOException;
import java.util.Map.Entry;

import org.apache.giraph.aggregators.AggregatorWriter;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * Default implementation of {@link AggregatorWriter}. Each line consists of
 * text and contains the aggregator name, the aggregator value and the
 * aggregator class.
 * 
 * We import most of the functionality from the Giraph implementation and
 * modify it to be more extensible.
 * 
 */
public class TextAggregatorWriter
    extends DefaultImmutableClassesGiraphConfigurable
    implements AggregatorWriter {
  /** The filename of the outputfile */
  public static final String FILENAME =
      "textAggregatorWriter.filename";
  /** Signal for "never write" frequency */
  public static final int NEVER = 0;
  /** Signal for "write only the final values" frequency */
  public static final int AT_THE_END = -1;
  /** Signal for "write values in every superstep" frequency */
  public static final int ALWAYS = 1;
  /** The frequency of writing:
   *  - NEVER: never write, files aren't created at all
   *  - AT_THE_END: aggregators are written only when the computation is over
   *  - int: i.e. 1 is every superstep, 2 every two supersteps and so on
   */
  public static final String FREQUENCY =
      "textAggregatorWriter.frequency";
  /** Default filename for dumping aggregator values */
  private static final String DEFAULT_FILENAME = "aggregatorValues";
  /** Handle to the outputfile */
  protected FSDataOutputStream output;
  /** Write every "frequency" supersteps */
  private int frequency;

  @Override
  @SuppressWarnings("rawtypes")
  public void initialize(Context context, long attempt) throws IOException {
    String s_frequency = getConf().get(FREQUENCY);
    if (s_frequency==null) {
      frequency = NEVER;
    } else if (s_frequency.toLowerCase().trim().equals("always")) {
      frequency = ALWAYS;
    } else if (s_frequency.toLowerCase().trim().equals("never")) {
      frequency = NEVER;
    } else if (s_frequency.toLowerCase().trim().equals("end")) {
      frequency = AT_THE_END;
    } else {
      frequency = getConf().getInt(FREQUENCY, NEVER);
    }
    String filename  = getConf().get(FILENAME, DEFAULT_FILENAME);
    if (frequency != NEVER) {
      Path p = new Path(filename + "_" + attempt);
      FileSystem fs = FileSystem.get(getConf());
      if (fs.exists(p)) {
        throw new RuntimeException("aggregatorWriter file already" +
            " exists: " + p.getName());
      }
      output = fs.create(p);
    }
  }

  @Override
  public void writeAggregator(
      Iterable<Entry<String, Writable>> aggregatorMap,
      long superstep) throws IOException {
    if (shouldWrite(superstep)) {
      for (Entry<String, Writable> entry : aggregatorMap) {
        byte[] bytes = aggregatorToString(entry.getKey(), entry.getValue(),
            superstep).getBytes(Charsets.UTF_8);
        output.write(bytes, 0, bytes.length);
      }
      output.flush();
    }
  }

  /**
   * Implements the way an aggregator is converted into a String.
   * Override this if you want to implement your own text format.
   *
   * @param aggregatorName Name of the aggregator
   * @param value Value of aggregator
   * @param superstep Current superstep
   * @return The String representation for the aggregator
   */
  protected String aggregatorToString(String aggregatorName,
      Writable value,
      long superstep) {
    return new StringBuilder("superstep=").append(superstep).append("\t")
        .append(aggregatorName).append("=").append(value).append("\n")
        .toString();
  }

  /**
   * Should write this superstep?
   *
   * @param superstep Superstep to check
   * @return True if should write, false otherwise
   */
  protected boolean shouldWrite(long superstep) {
    return (frequency == AT_THE_END && superstep == LAST_SUPERSTEP) ||
        (frequency != NEVER && superstep % frequency == 0 && frequency>0);
  }

  @Override
  public void close() throws IOException {
    if (output != null) {
      output.close();
    }
  }
  
  protected void setFrequency(int frequency) {
    this.frequency = frequency;
  }
}
