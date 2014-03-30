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
 * Election strategy that allows vertices to vote for a stage transition.
 *
 * The election is performed by collecting a count of votes for each
 * possible stage, and then applying the particular election rules.
 */
public interface TransitionElection {

  /**
   * Identifier for the aggregator that collects vertices' votes.
   */
  public static final String AGGREGATOR_VOTING = "multistage.voting";

  /**
   * Initializes this election type, usually by registering the
   * aggregator through which nodes can vote.
   *
   * @param master the multistage computation master.
   * @throws InstantiationException
   * @throws IllegalAccessException
   */
  public void initialize(MultistageMasterCompute master)
      throws InstantiationException, IllegalAccessException;

  /**
   * Resolves the election according to its rules, and changes the
   * stage if the election is successful.
   *
   * @param master the multistage computation master.
   */
  public void resolveElection(MultistageMasterCompute master);
}
