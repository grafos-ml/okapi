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

import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
* Affinity Propagation's MasterCompute.
*
* @author Josep Rubió Piqué <josep@datag.es>
*/
public class MasterComputation extends DefaultMasterCompute {
  private static Logger logger = LoggerFactory.getLogger(AffinityPropagation.class);

  @Override
  public void initialize() throws InstantiationException, IllegalAccessException {
    super.initialize();
    registerPersistentAggregator("exemplars", ExemplarAggregator.class);
    registerPersistentAggregator("converged", LongSumAggregator.class);
  }

  @Override
  public void compute() {
    logger.debug("Master computation at iteration {}", getSuperstep());
    super.compute();
  }
}