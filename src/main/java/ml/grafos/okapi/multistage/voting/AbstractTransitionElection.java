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
 * Skeletal implementation of a transition election, intended to
 * ease the implementation of different election types.
 */
public abstract class AbstractTransitionElection implements TransitionElection {

  /**
   * Initializes the election by registering the aggregator that
   * collects the votes of the vertices.
   *
   * @param master the multistage computation master.
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  @Override
  public void initialize(MultistageMasterCompute master)
      throws InstantiationException, IllegalAccessException {
    master.registerAggregator(AGGREGATOR_VOTING, VotingAggregator.class);
  }

  /**
   * Collects the votes and, if there is more than one, calls
   * {@link #resolveElection(ml.grafos.okapi.multistage.MultistageMasterCompute, com.google.common.collect.Multiset)}
   * to resolve the election.
   *
   * @param master the multistage computation master.
   */
  @Override
  public final void resolveElection(MultistageMasterCompute master) {
    final IntMultisetWrapperWritable votingAggregator =
        master.getAggregatedValue(AGGREGATOR_VOTING);

    final Multiset<Integer> votes = votingAggregator.get();
    if (votes.size() == 0) {
      return;
    }

    resolveElection(master, votes);
  }

  /**
   * Resolves an election according to the specified vote counts.
   *
   * @param master the multistage computation master.
   * @param votes bag of vote counts for each stage.
   */
  protected abstract void resolveElection(MultistageMasterCompute master,
                                          Multiset<Integer> votes);
}
