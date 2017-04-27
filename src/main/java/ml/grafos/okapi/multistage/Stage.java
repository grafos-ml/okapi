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
package ml.grafos.okapi.multistage;

import org.apache.giraph.graph.Computation;

/**
 * Holds the definition of a stage in a multistage computation.
 */
public interface Stage {

  /**
   * Get the class of the computation that vertices should run
   * when executing this stage.
   *
   * @return computation class.
   */
  public Class<? extends StageComputation> getStageClass();
}
