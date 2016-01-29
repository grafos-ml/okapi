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

import org.apache.giraph.io.formats.TextVertexValueInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.jblas.util.Random;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Vertex input formatter for Affinity Propagation problems.
 * <p/>
 * The input format consists of an entry for each of the data points to cluster.
 * The first element of the entry is an integer value encoding the data point
 * index (id). Subsequent elements in the entry are double values encoding the
 * similarities between the data point of the current entry and the rest of
 * data points in the problem.
 * <p/>
 * Example:<br/>
 * 1 1 1 5
 * 2 1 1 3
 * 3 5 3 1
 * <p/>
 * Encodes a problem in which data point "1" has similarity 1 with itself,
 * 1 with point "2" and 5 with point "3". In a similar manner, points "2",
 * and "3" have similarities of [1, 1, 3] and [5, 3, 1] respectively with
 * points "1", "2", and "3".
 *
 * @author Josep Rubió Piqué <josep@datag.es>
 */
public class APVertexInputFormatter
    extends TextVertexValueInputFormat<APVertexID, APVertexValue, DoubleWritable> {

  private static final Pattern SEPARATOR = Pattern.compile("[\001\t ]");

  @Override
  public TextVertexValueReader createVertexValueReader(InputSplit split, TaskAttemptContext context) throws IOException {
    return new APInputReader();
  }

  public class APInputReader extends TextVertexValueReaderFromEachLineProcessed<String[]> {

    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      return SEPARATOR.split(line.toString());
    }

    @Override
    protected APVertexID getId(String[] line) throws IOException {
      return new APVertexID(APVertexType.I,
          Long.valueOf(line[0]));
    }

    @Override
    protected APVertexValue getValue(String[] line) throws IOException {
      APVertexValue value = new APVertexValue();
      for (int i = 1; i < line.length; i++) {
        APVertexID neighId = new APVertexID(APVertexType.E, i);
        value.weights.put(neighId, new DoubleWritable(Double.valueOf(line[i])
            // noise
            + Random.nextDouble()*getConf().getFloat(AffinityPropagation.NOISE, AffinityPropagation.NOISE_DEFAULT)));
      }
      return value;
    }
  }
}