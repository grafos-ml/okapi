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
package ml.grafos.okapi.common.data;

import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class IntArrayListWritable
  extends ArrayListWritable<IntWritable>
  implements WritableComparable<IntArrayListWritable> {
  /** Default constructor for reflection */
  public IntArrayListWritable() {
    super();
  }

  @Override
  public void setClass() {
    setClass(IntWritable.class);
  }

  /**
   * public int compareTo(MessageWrapper wrapper) { if (this == wrapper )
   * return 0;
   *
   * if (this.sourceId.compareTo(wrapper.getSourceId()) == 0) return
   * this.message.compareTo(wrapper.getMessage()); 
   * else return this.sourceId.compareTo(wrapper.getSourceId()); }
   *
   * @param message Message to be compared
   * @return 0 value
   */
  @Override
  public int compareTo(IntArrayListWritable message) {
    if (message==null) {
      return 1;
    }
    if (this.size()<message.size()) {
      return -1;
    }
    if (this.size()>message.size()) {
      return 1;
    }
    for (int i=0; i<this.size(); i++) {
      if (this.get(i)==null && message.get(i)==null) {
        continue;
      }
      if (this.get(i)==null) {
        return -1;
      }
      if (message.get(i)==null) {
        return 1;
      }
      if (this.get(i).get()<message.get(i).get()) {
        return -1;
      }
      if (this.get(i).get()>message.get(i).get()) {
        return 1;
      }
    }
    return 0;
  }
}
