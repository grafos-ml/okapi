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
package ml.grafos.okapi.multistage.voting;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Wraps a {@link com.google.common.collect.Multiset} of Integers as a
 * {@link org.apache.hadoop.io.Writable} element.
 */
public class IntMultisetWrapperWritable implements Writable {

  private Multiset<Integer> votes = HashMultiset.create();

  public Multiset<Integer> get() {
    return votes;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    final int nDistinctElements = votes.elementSet().size();
    dataOutput.writeInt(nDistinctElements);
    for (Integer element : votes.elementSet()) {
      dataOutput.writeInt(element);
      dataOutput.writeInt(votes.count(element));
    }
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    final int nDistinctElements = dataInput.readInt();
    for (int i=0; i<nDistinctElements; i++) {
      votes.add(dataInput.readInt(), dataInput.readInt());
    }
  }
}
