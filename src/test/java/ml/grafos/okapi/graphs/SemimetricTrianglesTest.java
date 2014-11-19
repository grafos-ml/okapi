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

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.List;

import ml.grafos.okapi.io.formats.LongDoubleTextEdgeInputFormat;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.HashMapEdges;
import org.apache.giraph.io.formats.AdjacencyListTextVertexOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

public class SemimetricTrianglesTest {
	
	final double delta = 0.0001;

  @Test
  public void testSemimetricRemoval() {
    String[] graph = { 
        "1 2 10.0",
        "1 4 1.0",
        "2 3 3.0",
        "2 4 2.0",
        "2 5 2.0",
        "3 5 1.0",
        "3 6 5.0",
        "5 6 3.0",
        "2 1 10.0",
        "4 1 1.0",
        "3 2 3.0",
        "4 2 2.0",
        "5 2 2.0",
        "5 3 1.0",
        "6 3 5.0",
        "6 5 3.0"
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(SemimetricTriangles.PropagateId.class);
    conf.setMasterComputeClass(SemimetricTriangles.SemimetricMasterCompute.class);
    conf.setEdgeInputFormatClass(LongDoubleTextEdgeInputFormat.class);
    conf.setVertexOutputFormatClass(AdjacencyListTextVertexOutputFormat.class);
    conf.setOutEdgesClass(HashMapEdges.class);
    
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
      
      String[] output = string.split("[\t ]");
      if (Integer.parseInt(output[0]) == 1) {
    	  // check that semi-metric edge (1, 2) has been removed
    	  assertEquals(output.length, 4);
    	  assertEquals(4, Integer.parseInt(output[2]));
      }
      if (Integer.parseInt(output[0]) == 3) {
    	  // check that semi-metric edge (3, 6) has been removed
    	  assertEquals(output.length, 6);
    	  assertEquals(2, Integer.parseInt(output[2]));
      }
    }
  }
}