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
 * Election where all vertices must vote, and vote for the same transition to
 * change the computation stage.
 *
 * <p>If multiple vertices vote for a different transition, the election fails with
 * an {@link java.lang.IllegalStateException}. If all emitted votes are for the
 * same transition but not all vertices vote, the election does not fail but
 * no transition takes place.
 *
 * <p>This election type handles stage transitions similar to how
 * {@link org.apache.giraph.graph.Vertex#voteToHalt()} is handled. Unfortunately,
 * transition votes do not persist between supersteps due to technical limitations.
 * Hence, your vertex must re-emit its vote each superstep until unanimity is
 * reached. A common pattern to achieve that is to include a "done for this step"
 * check at the beginning of a {@link ml.grafos.okapi.multistage.StageComputation}
 * that directly emits a transition vote when no messages are received:
 *
 * <pre>
 * {@code
 * if (!(messages.iterator().hasNext()) {
 *     voteToTransition(Stages.NEXT_STAGE);
 *     return;
 * }
 * }
 * </pre>
 */
public class UnanimityElection extends AbstractTransitionElection {
  @Override
  protected void resolveElection(MultistageMasterCompute master, Multiset<Integer> votes) {
    if (votes.entrySet().size() > 1) {
      throw new IllegalStateException(
          "Multiple vertices voted for different transition in UNANIMITY voting mode.");
    }
    if (votes.size() == master.getTotalNumVertices()) {
      master.setStage(votes.elementSet().iterator().next());
    }
  }
}
