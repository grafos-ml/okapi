package es.tid.graphlib.cf.svd;

import java.io.IOException;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import es.tid.graphlib.svd.DoubleArrayListHashMapDoubleWritable;
import es.tid.graphlib.svd.Svdpp;
import es.tid.graphlib.svd.TextIntDoubleArrayVertexWriter;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for graphs
 * with int ids.
 *
 * Each line consists of: vertex id, vertex value and option edge value
 */
public class TextDoubleArrayHashMapDoubleTextVertexOutputFormat
  extends TextVertexOutputFormat
  <Text, DoubleArrayListHashMapDoubleWritable, DoubleWritable> {

  /** Specify the output delimiter. */
  public static final String LINE_TOKENIZE_VALUE = "output.delimiter";
  /** Default output delimiter. */
  public static final String LINE_TOKENIZE_VALUE_DEFAULT = "   ";
  /**
   * Create Vertex Writer.
   *
   * @param context Context
   * @return new object TextIntIntVertexWriter
   */
  public final TextVertexWriter
  createVertexWriter(final TaskAttemptContext context) {
    return new TextIntDoubleArrayVertexWriter();
  }
  /** Class TextIntIntVertexWriter. */
  protected class TextIntDoubleArrayVertexWriter
      extends TextVertexWriterToEachLine {
    /** Saved delimiter. */
    private String delimiter;

    @Override
    public final void initialize(final TaskAttemptContext context)
      throws IOException, InterruptedException {
      super.initialize(context);
      Configuration conf = context.getConfiguration();
      delimiter = conf
        .get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
    }

    @Override
    protected final Text convertVertexToLine(final Vertex
      <Text, DoubleArrayListHashMapDoubleWritable, DoubleWritable, ?>
      vertex)
      throws IOException {
      boolean isErrorFlag = getContext().getConfiguration().getBoolean(
        "svd.print.error", false);
      boolean isUpdatesFlag = getContext().getConfiguration().getBoolean(
        "svd.print.updates", false);
      boolean isMessagesFlag = getContext().getConfiguration().getBoolean(
        "svd.print.messages", false);

      String type = "";
      if (((Svdpp) vertex).isItem()) {
        type = "item";
      } else {
        type = "user";
      }
      String id = vertex.getId().toString();
      String value = vertex.getValue().getLatentVector().toString();
      String error = null;
      String updates = null;
      String messages = null;
      Text line = new Text(type + delimiter + id + delimiter + value);

      if (isErrorFlag) {
        error = Double.toString(Math.abs(((Svdpp) vertex).getHaltFactor()));
        line.append(delimiter.getBytes(), 0, delimiter.length());
        line.append(error.getBytes(), 0, error.length());
      }
      if (isUpdatesFlag) {
        updates = Integer.toString(((Svdpp) vertex).getUpdates());
        line.append(delimiter.getBytes(), 0, delimiter.length());
        line.append(updates.getBytes(), 0, updates.length());
      }
      if (isMessagesFlag) {
        messages = Integer.toString(((Svdpp) vertex).getMessages());
        line.append(delimiter.getBytes(), 0, delimiter.length());
        line.append(messages.getBytes(), 0, messages.length());
      }
      return new Text(line);
    }
  }
}