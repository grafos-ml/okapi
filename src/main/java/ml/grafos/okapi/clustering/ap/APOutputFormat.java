/**
 * Copyright 2014 Grafos.ml
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
package ml.grafos.okapi.clustering.ap;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Output Formatter for Affinity Propagation problems.
 * <p/>
 * The output format consists of an entry for each of the data points to cluster.
 * The first element of the entry is a integer value encoding the data point
 * index (id), whereas the second value encodes the exemplar id chosen for
 * that point.
 * <p/>
 * Example:<br/>
 * 1 3
 * 2 3
 * 3 3
 * <p/>
 * Encodes a solution in which data points "1", "2", and "3" choose point "3"
 * as an exemplar.
 *
 * @author Marc Pujol-Gonzalez <mpujol@iiia.csic.es>
 * @author Toni Penya-Alba <tonipenya@iiia.csic.es>
 */
@SuppressWarnings("rawtypes")
public class APOutputFormat
    extends IdWithValueTextOutputFormat<APVertexID,APVertexValue, DoubleWritable> {

  /**
   * Specify the output delimiter
   */
  public static final String LINE_TOKENIZE_VALUE = "output.delimiter";
  /**
   * Default output delimiter
   */
  public static final String LINE_TOKENIZE_VALUE_DEFAULT = "\t";

  @Override
  public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
    return new IdWithValueVertexWriter();
  }

  protected class IdWithValueVertexWriter extends TextVertexWriterToEachLine {
    /**
     * Saved delimiter
     */
    private String delimiter;

    @Override
    public void initialize(TaskAttemptContext context) throws IOException,
        InterruptedException {
      super.initialize(context);
      delimiter = getConf().get(
          LINE_TOKENIZE_VALUE, LINE_TOKENIZE_VALUE_DEFAULT);
    }

    @Override
    protected Text convertVertexToLine(Vertex<APVertexID,
        APVertexValue, DoubleWritable> vertex)
        throws IOException {

      if (vertex.getId().type != APVertexType.ROW) {
        return null;
      }

      return new Text(String.valueOf(vertex.getId().index)
          + delimiter + Long.toString(vertex.getValue().exemplar.get()));
    }
  }
}
