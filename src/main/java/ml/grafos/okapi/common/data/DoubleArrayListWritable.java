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
package ml.grafos.okapi.common.data;

import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;

public class DoubleArrayListWritable
  extends ArrayListWritable<DoubleWritable>
  implements WritableComparable<DoubleArrayListWritable>{

  /**
   * Serialization number.
   */
  private static final long serialVersionUID = 5968423975161632117L;

  /** Default constructor for reflection */
  public DoubleArrayListWritable() {
    super();
  }

  /**
   * Constructor
   *
   * @param other Other
   */
  public DoubleArrayListWritable(DoubleArrayListWritable other) {
    super(other);
  }

  @Override
  public void setClass() {
    setClass(DoubleWritable.class);
  }

  /**
   * Print the Double Array List
   */
  public void print() {
    System.out.print("[");
    for (int i = 0; i < this.size(); i++) {
      System.out.print(this.get(i));
      if (i < this.size() - 1) {
        System.out.print("; ");
      }
    }
    System.out.println("]");
  }

  /**
   * Compare function
   *
   * @param other Other array to be compared
   *
   * @return 0
   */
  public int compareTo(DoubleArrayListWritable other) {
    if (other==null) {
      return 1;
    }
    if (this.size() < other.size()) {
      return -1;
    }
    if (this.size() > other.size()) {
      return 1;
    }
    for (int i=0; i < this.size(); i++) {
      if (this.get(i) == null && other.get(i) == null) {
        continue;
      }
      if (this.get(i) == null) {
        return -1;
      }
      if (other.get(i) == null) {
        return 1;
      }
      if (this.get(i).get() < other.get(i).get()) {
        return -1;
      }
      if (this.get(i).get() > other.get(i).get()) {
        return 1;
      }
    }
    return 0;
  }
}
