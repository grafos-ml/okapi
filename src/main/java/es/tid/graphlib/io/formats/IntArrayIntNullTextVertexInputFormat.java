package es.tid.graphlib.io.formats;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.giraph.edge.DefaultEdge;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.examples.SimpleTriangleClosingVertex;
import org.apache.giraph.examples.SimpleTriangleClosingVertex.IntArrayListWritable;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;

/**
 * Simple text-based {@link org.apache.giraph.io.VertexInputFormat} for
 * unweighted graphs with int ids and values that are IntArrayWritables.
 *
 * This is for use with the
 * {@link org.apache.giraph.examples.SimpleTriangleClosingVertex}.
 *
 * Each line consists of: vertex neighbor1 neighbor2 ...
 */
public class IntArrayIntNullTextVertexInputFormat extends
    TextVertexInputFormat<IntWritable, IntArrayListWritable, NullWritable> {
  /** Separator of the vertex and neighbors */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context)
    throws IOException {
    return new IntArrayIntNullVertexReader();
  }

  /**
   * Vertex reader associated with {@link IntArrayIntNullTextVertexInputFormat}.
   */
  public class IntArrayIntNullVertexReader extends
    TextVertexReaderFromEachLineProcessed<String[]> {
    /**
     * Cached vertex id for the current line
     */
    private IntWritable id;

    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      id = new IntWritable(Integer.parseInt(tokens[0]));
      return tokens;
    }

    @Override
    protected IntWritable getId(String[] tokens) throws IOException {
      return id;
    }

    @Override
    protected IntArrayListWritable getValue(String[] tokens)
      throws IOException {
      return new SimpleTriangleClosingVertex.IntArrayListWritable();
    }

    @Override
    protected Iterable<Edge<IntWritable, NullWritable>> getEdges(
        String[] tokens) throws IOException {
      List<Edge<IntWritable, NullWritable>> edges =
          Lists.newArrayListWithCapacity(tokens.length - 1);
      for (int n = 1; n < tokens.length; n++) {
        DefaultEdge<IntWritable, NullWritable> edge =
          new DefaultEdge<IntWritable, NullWritable>();
        edge.setTargetVertexId(new IntWritable(Integer.parseInt(tokens[n])));
        edge.setValue(NullWritable.get());
        edges.add(edge);
      }
      return edges;
    }
  }
}
