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

import ml.grafos.okapi.common.data.MapWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
* Value stored in Affinity Propagation vertices.
*
* @author Josep Rubió Piqué <josep@datag.es>
*/
public class APVertexValue implements Writable {
  /**
   * Id of the exemplar chosen by the vertex (if row vertex).
   */
  public LongWritable exemplar;
  /**
   * Similarity between this point and its neighbors (if row vertex.)
   */
  public MapWritable weights;
  /**
   * Last messages sent from this vertex to its neighbors.
   */
  public MapWritable lastSentMessages;

  /**
   * Last messages this vertex received.
   */
  public MapWritable lastReceivedMessages;

  /**
   * Has converged.
   */
  public BooleanWritable converged;

  /**
   * Exemplar.
   */
  public BooleanWritable exemplarCalc;

  public APVertexValue() {
    exemplar = new LongWritable();
    weights = new MapWritable();
    lastSentMessages = new MapWritable();
    lastReceivedMessages = new MapWritable();
    converged = new BooleanWritable();
    exemplarCalc = new BooleanWritable();
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    exemplar.write(dataOutput);
    weights.write(dataOutput);
    lastSentMessages.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    exemplar.readFields(dataInput);
    weights.readFields(dataInput);
    lastSentMessages.readFields(dataInput);
  }
}