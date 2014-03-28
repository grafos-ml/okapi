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
package ml.grafos.okapi.graphs;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

import static org.junit.Assert.fail;

public class AffinityPropagationTest {

  @Test
  public void test() {
    String[] graph = {
      "1 1 1 5",
      "2 1 1 3",
      "3 5 3 1",
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(AffinityPropagation.class);
    conf.setMasterComputeClass(AffinityPropagation.MasterComputation.class);
    conf.setVertexInputFormatClass(AffinityPropagation.APInputFormatter.class);
    conf.setInt(AffinityPropagation.MAX_ITERATIONS, 15);
    conf.setFloat(AffinityPropagation.DAMPING, 0.9f);
    conf.setVertexOutputFormatClass(AffinityPropagation.APOutputFormat.class);
    conf.setMaxNumberOfSupersteps(200);
    Iterable<String> results;
    try {
      results = InternalVertexRunner.run(conf, graph, null);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception occurred");
      return;
    }

    System.err.println(results);
  }

}
