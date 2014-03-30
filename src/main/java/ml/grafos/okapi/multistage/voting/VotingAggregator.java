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

import com.google.common.collect.Multiset;
import org.apache.giraph.aggregators.BasicAggregator;

/**
 * Aggregator of vertex votes for a state change.
 *
 * This aggregator is defined as the commutative monoid of integer multisets with
 * multiset union. That is:
 * <ul>
 * <li>- elements: integer multisets</li>
 * <li>- neutral element: empty multiset</li>
 * <li>- binary operator (aggregator): multiset union</li>
 * </ul>
 */
public class VotingAggregator extends BasicAggregator<IntMultisetWrapperWritable> {

  @Override
  public void aggregate(IntMultisetWrapperWritable value) {
    Multiset<Integer> votes = getAggregatedValue().get();
    votes.addAll(value.get());
  }

  @Override
  public IntMultisetWrapperWritable createInitialValue() {
    return new IntMultisetWrapperWritable();
  }

}
