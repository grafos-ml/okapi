/**
 * Copyright 2013 Grafos.ml
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
package ml.grafos.okapi.clustering;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;
import ml.grafos.okapi.graphs.SemiClustering;
import ml.grafos.okapi.io.formats.LongDoubleTextEdgeInputFormat;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

public class SemiClusteringTest {

  @Test
  public void test() {
    String[] graph = { 
        "1 2 1.0",
        "2 1 1.0",
        "1 3 1.0",
        "3 1 1.0",
        "2 3 2.0",
        "3 2 2.0",
        "3 4 2.0",
        "4 3 2.0",
        "3 5 1.0",
        "5 3 1.0",
        "4 5 1.0",
        "5 4 1.0"
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(SemiClustering.class);
    conf.setEdgeInputFormatClass(LongDoubleTextEdgeInputFormat.class);
    conf.setInt(SemiClustering.ITERATIONS, 10);
    conf.setInt(SemiClustering.MAX_CLUSTERS, 2);
    conf.setInt(SemiClustering.CLUSTER_CAPACITY, 2);
    conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);
    Iterable<String> results;
    try {
      results = InternalVertexRunner.run(conf, null, graph);
    } catch (Exception e) {
      e.printStackTrace();
      fail("Exception occurred");
      return;
    }
    List<String> res = new LinkedList<String>();
    for (String string : results) {
      res.add(string);
      System.out.println(string);
    }
    Assert.assertEquals(5, res.size());
  }

}
