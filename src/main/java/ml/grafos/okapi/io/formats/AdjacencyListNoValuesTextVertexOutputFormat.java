/**
 * Copyright 2013 Grafos.ml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.grafos.okapi.io.formats;

import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.giraph.io.formats.AdjacencyListTextVertexOutputFormat;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * OutputFormat to write out the graph nodes as text, value-separated (by
 * tabs, by default).  With the default delimiter, a vertex is written out as:
 *
 * <VertexId>[<tab><EdgeId>]+
 *
 * This is similar to {@link AdjacencyListTextVertexOutputFormat}, only it does
 * not write values.
 *
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class AdjacencyListNoValuesTextVertexOutputFormat<I extends WritableComparable,
    V extends Writable, E extends Writable>
    extends TextVertexOutputFormat<I, V, E> {
  /** Split delimiter */
  public static final String LINE_TOKENIZE_VALUE = "output.delimiter";
  /** Default split delimiter */
  public static final String LINE_TOKENIZE_VALUE_DEFAULT =
    AdjacencyListTextVertexInputFormat.LINE_TOKENIZE_VALUE_DEFAULT;

  @Override
  public AdjacencyListTextVertexWriter createVertexWriter(
      TaskAttemptContext context) {
    return new AdjacencyListTextVertexWriter();
  }

  /**
   * Vertex writer associated with {@link AdjacencyListNoValuesTextVertexOutputFormat}.
   */
  protected class AdjacencyListTextVertexWriter extends
    TextVertexWriterToEachLine {
    /** Cached split delimeter */
    private String delimiter;

    @Override
    public void initialize(TaskAttemptContext context) throws IOException,
        InterruptedException {
      super.initialize(context);
      delimiter =
          getConf().get(LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
    }

    @Override
    public Text convertVertexToLine(Vertex<I, V, E> vertex)
      throws IOException {
      StringBuffer sb = new StringBuffer(vertex.getId().toString());
      sb.append(delimiter);

      for (Edge<I, E> edge : vertex.getEdges()) {
        sb.append(delimiter).append(edge.getTargetVertexId());
      }

      return new Text(sb.toString());
    }
  }

}
