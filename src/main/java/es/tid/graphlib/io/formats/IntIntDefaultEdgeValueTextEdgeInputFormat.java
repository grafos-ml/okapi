package es.tid.graphlib.io.formats;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.IntNullTextEdgeInputFormat;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.giraph.utils.IntPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for
 * weighted graphs with int ids int values. In this format however,
 * the value of the edge is not expected in the input.
 *
 * Each line consists of: <src id> <dst id>
 */
public class IntIntDefaultEdgeValueTextEdgeInputFormat extends
    TextEdgeInputFormat<IntWritable, IntWritable> {
  /** Splitter for endpoints */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  @Override
  public EdgeReader<IntWritable, IntWritable> createEdgeReader(
      InputSplit split, TaskAttemptContext context) throws IOException {
    return new IntIntTextEdgeReader();
  }

  /**
   * {@link org.apache.giraph.io.EdgeReader} associated with
   * {@link IntNullTextEdgeInputFormat}.
   */
  public class IntIntTextEdgeReader extends
      TextEdgeReaderFromEachLineProcessed<String[]> {
    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      return SEPARATOR.split(line.toString());
    }

    @Override
    protected IntWritable getSourceVertexId(String[] tokens)
      throws IOException {
      return new IntWritable(Integer.parseInt(tokens[0]));
    }

    @Override
    protected IntWritable getTargetVertexId(String[] tokens)
      throws IOException {
      return new IntWritable(Integer.parseInt(tokens[1]));
    }

    @Override
    protected IntWritable getValue(String tokens[]) throws IOException {
      return new IntWritable(-1);
    }
  }
}
