package ml.grafos.okapi.cf;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.IntNullTextEdgeInputFormat;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} to read
 * user-item ratings as the input for the CF algorithms.
 *
 * Each line consists of: <user id> <item id> <rating (float)>
 */
public class CfLongIdFloatTextInputFormat extends
    TextEdgeInputFormat<CfLongId, FloatWritable> {
  /** Splitter for endpoints */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  @Override
  public EdgeReader<CfLongId, FloatWritable> createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new CfIdFloatTextEdgeReader();
  }

  /**
   * {@link org.apache.giraph.io.EdgeReader} associated with
   * {@link IntNullTextEdgeInputFormat}.
   */
  public class CfIdFloatTextEdgeReader extends
      TextEdgeReaderFromEachLineProcessed<String[]> {
    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      return SEPARATOR.split(line.toString());
    }

    @Override
    protected CfLongId getSourceVertexId(String[] tokens)
      throws IOException {
      // type 0 is user
      return new CfLongId((byte)0, Integer.parseInt(tokens[0]));
    }

    @Override
    protected CfLongId getTargetVertexId(String[] tokens)
      throws IOException {
      // type 1 is item
      return new CfLongId((byte)1, Integer.parseInt(tokens[1]));
    }

    @Override
    protected FloatWritable getValue(String[] tokens) throws IOException {
      return new FloatWritable(Float.parseFloat(tokens[2]));
    }
  }
}
