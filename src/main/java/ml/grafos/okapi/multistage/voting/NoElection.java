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

import ml.grafos.okapi.multistage.MultistageMasterCompute;

/**
 * Dummy election that just does not happen.
 *
 * When using this election type, vertices are not allowed to vote
 * for transition changes.
 *
 * This election type should be employed when only the master compute
 * should decide when stage transitions happen. This is the typical
 * case on algorithms where stage transitions are purely based on the
 * superstep numbers, or when the master compute uses some aggregated
 * information to make the decision.
 */
public class NoElection implements TransitionElection {
  @Override
  public void initialize(MultistageMasterCompute master)
      throws InstantiationException, IllegalAccessException{}

  @Override
  public void resolveElection(MultistageMasterCompute master) {}
}
