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

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This class defines the messages exchanged between Affinity propagation vertices.
 *
 * @author Josep Rubió Piqué <josep@datag.es>
*/
public class APMessage implements Writable {

  public APVertexID from;
  public double value;

  public APMessage() {
    from = new APVertexID();
  }

  public APMessage(APVertexID from, double value) {
    this.from = from;
    this.value = value;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    from.write(dataOutput);
    dataOutput.writeDouble(value);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    from.readFields(dataInput);
    value = dataInput.readDouble();
  }

  @Override
  public String toString() {
    return "APMessage{from=" + from + ", value=" + value + '}';
  }
}