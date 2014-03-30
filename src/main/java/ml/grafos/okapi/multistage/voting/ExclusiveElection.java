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
import ml.grafos.okapi.multistage.MultistageMasterCompute;

/**
 * In exclusive mode only one vertex can vote to change the stage,
 * and its decision will be respected. If multiple vertices emit a
 * vote, then the computation will fail with an
 * {@link IllegalStateException}.
 */
public class ExclusiveElection extends AbstractTransitionElection {

  @Override
  protected void resolveElection(MultistageMasterCompute master,
                                 Multiset<Integer> votes) {
    if (votes.size() > 1) {
      throw new IllegalStateException(
          "Multiple vertices voted for a stage change in EXCLUSIVE voting mode.");
    }

    master.setStage(votes.elementSet().iterator().next());
  }

}
