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

import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
* Id for Affinity propagation vertices.
*
* @author Josep Rubió Piqué <josep@datag.es>
*/
public class APVertexID implements WritableComparable<APVertexID> {

  /**
   * Identifies whether the vertex represents a or or a column factor.
   */
  public APVertexType type = APVertexType.I;
  /**
   * Index of the point to be clustered.
   */
  public long index = 0;

  public APVertexID() {
  }

  public APVertexID(APVertexID orig) {
    this(orig.type, orig.index);
  }

  public APVertexID(APVertexType type, long index) {
    this.type = type;
    this.index = index;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    dataOutput.writeInt(type.ordinal());
    dataOutput.writeLong(index);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    final int typeIndex = dataInput.readInt();
    type = APVertexType.values()[typeIndex];
    this.index = dataInput.readLong();
  }

  @Override
  public int compareTo(APVertexID that) {
    return ComparisonChain.start()
        .compare(this.type, that.type)
        .compare(this.index, that.index)
        .result();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    APVertexID that = (APVertexID) o;
    return index == that.index && type == that.type;
  }

  @Override
  public int hashCode() {
    int result = (int) (index ^ (index >>> 32));
    result = 31 * result + type.ordinal();
    return result;
  }

  @Override
  public String toString() {
    return "(" + type + ", " + index + ")";
  }
}