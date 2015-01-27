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

import ml.grafos.okapi.common.data.LongArrayListWritable;
import org.apache.giraph.aggregators.BasicAggregator;

/**
* Aggregates points that have chosen themselves as an exemplar.
 *
* @author Toni Penya-Alba <tonipenya@iiia.csic.es>
* @author Marc Pujol-Gonzalez <mpujol@iiia.csic.es>
*/
public class ExemplarAggregator extends BasicAggregator<LongArrayListWritable> {

  @Override
  public void aggregate(LongArrayListWritable value) {
    getAggregatedValue().addAll(value);
  }

  @Override
  public LongArrayListWritable createInitialValue() {
    return new LongArrayListWritable();
  }
}
