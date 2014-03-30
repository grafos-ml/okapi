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

import ml.grafos.okapi.multistage.voting.IntMultisetWrapperWritable;
import ml.grafos.okapi.multistage.voting.TransitionElection;
import ml.grafos.okapi.multistage.voting.VotingAggregator;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.IOException;

/**
 * Skeletal implementation of a stage part of a multistage computation.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M1> Incoming message type
 * @param <M2> Outgoing message type
 */
public abstract class StageComputation<I extends WritableComparable,
    V extends Writable, E extends Writable, M1 extends Writable, M2 extends Writable>
    extends AbstractComputation<I, V, E, M1, M2> {

  /**
   * Emits a vote to transition into the specified state.
   *
   * The result of the vote emission depends on the multistage master voting
   * mode.
   *
   * @see ml.grafos.okapi.multistage.voting.TransitionElection
   * @param stage stage this vertex wishes to transition to.
   */
  public void voteToTransition(Enum<?> stage) {
    final IntMultisetWrapperWritable value = new IntMultisetWrapperWritable();
    value.get().add(stage.ordinal());
    aggregate(TransitionElection.AGGREGATOR_VOTING, value);
  }

}
