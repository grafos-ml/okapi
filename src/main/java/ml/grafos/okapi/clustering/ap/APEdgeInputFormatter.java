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

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Edge input formatter for Affinity Propagation problems.
 * <p/>
 * The input format consists of an entry for each pair of points. The first
 * element of the entry denotes the id of the first point. Similarly, the
 * second element denotes the id of the second point. Finally, the third
 * element contains a double value encoding the similarity between the first
 * and second points.
 * <p/>
 * Example:<br/>
 * 1 1 1
 * 1 2 1
 * 1 3 5
 * 2 1 1
 * 2 2 1
 * 2 3 3
 * 3 1 5
 * 3 2 3
 * 3 3 1
 * <p/>
 * Encodes a problem in which data point "1" has similarity 1 with itself,
 * 1 with point "2" and 5 with point "3". In a similar manner, points "2",
 * and "3" have similarities of [1, 1, 3] and [5, 3, 1] respectively with
 * points "1", "2", and "3".
 *
 * @author Josep Rubió Piqué <josep@datag.es>
 */
public class APEdgeInputFormatter extends TextEdgeInputFormat<APVertexID, DoubleWritable> {

  private static final Pattern SEPARATOR = Pattern.compile("[\001\t ]");

  @Override
  public EdgeReader<APVertexID, DoubleWritable> createEdgeReader(InputSplit split, TaskAttemptContext context) throws IOException {
    return new APEdgeInputReader();
  }

  public class APEdgeInputReader extends TextEdgeReaderFromEachLineProcessed<String []> {

    @Override
    protected String[] preprocessLine(Text line) throws IOException {
      return SEPARATOR.split(line.toString());
    }

    @Override
    protected APVertexID getTargetVertexId(String[] line) throws IOException {
      return new APVertexID(APVertexType.E, Long.valueOf(line[1]));
    }

    @Override
    protected APVertexID getSourceVertexId(String[] line) throws IOException {
      return new APVertexID(APVertexType.I, Long.valueOf(line[0]));
    }

    @Override
    protected DoubleWritable getValue(String[] line) throws IOException {
      return new DoubleWritable(Double.valueOf(line[2])
          + getConf().getFloat(AffinityPropagation.NOISE, AffinityPropagation.NOISE_DEFAULT));
    }
  }
}