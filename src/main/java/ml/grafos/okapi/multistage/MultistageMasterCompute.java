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

import ml.grafos.okapi.multistage.voting.TransitionElection;
import org.apache.giraph.aggregators.IntOverwriteAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.IntWritable;

/**
 * Base class for masters of multi-stage computations.
 *
 * <p>This class eases implementation of algorithms that require multiple
 * stages by providing facilites to define the computation's stages
 * and rules on how to transition between them.
 *
 * @author Marc Pujol-Gonzalez <mpujol@iiia.csic.es>
 */
public abstract class MultistageMasterCompute extends DefaultMasterCompute {

  @Override
  public void initialize() throws InstantiationException, IllegalAccessException {
    super.initialize();
    getTransitionElection().initialize(this);
  }

  @Override
  public void compute() {
    super.compute();

    if (getSuperstep() > 0) {
      getTransitionElection().resolveElection(this);
    }
  }

  /**
   * Get the transition election employed in this multistage computation.
   *
   * <p>This method must always return the same value for a given master
   * (as if it was a final static member).
   *
   * @see ml.grafos.okapi.multistage.voting.TransitionElection
   * @return transition election of this computation.
   */
  public abstract TransitionElection getTransitionElection();

  /**
   * Set the computation stage that will be executed next.
   *
   * <p>When set in the {@link #initialize()} method, this sets the initial stage. When
   * set in the {@link #compute()} method, this is the stage that vertices will run
   * in the current superstep.
   *
   * @param stage stage to execute next.
   */
  public void setStage(Stage stage) {
    setComputation(stage.getStageClass());
  }

  /**
   * Set the computation stage that will be executed next.
   *
   * @see #setStage(Stage)
   * @param stageIndex stage to execute next, as an index into the array returned by
   *                   {@link #getStages()}.
   */
  public void setStage(int stageIndex) {
    setStage(getStages()[stageIndex]);
  }

  /**
   * Get the stages available for the multistage computation.
   *
   * <p>This method must always return the same value for a given master
   * (as if it was a final static member).
   *
   * <p>It is highly recommended that you employ an enumerator to store
   * the list of available stages. Then you can use the <code>values()</code>
   * method of the enum to build the return value of this method. For example:
   * <pre>
   * {@code
   * public static enum AlgorithmStages implements Stage {
   *     STAGE1(Stage1Computation.class),
   *     STAGE2(Stage2Computation.class);
   *
   *     private final Class<? extends StageComputation> clazz;
   *
   *     AffinityStages(Class<? extends StageComputation> clazz) {
   *         this.clazz = clazz;
   *     }
   *
   *     {@literal @}Override
   *     public Class<? extends StageComputation> getStageClass() {
   *         return clazz;
   *     }
   * }
   * }
   * </pre>
   * Then, you can implement this method as a simple:
   * <pre>
   * {@code
   *     return AlgorithmStages.values();
   * }
   * </pre>
   *
   * @return computation stages that may be executed.
   */
  public abstract Stage[] getStages();

}