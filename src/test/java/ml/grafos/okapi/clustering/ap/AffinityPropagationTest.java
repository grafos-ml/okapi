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
package ml.grafos.okapi.clustering.ap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.google.common.io.Resources;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class AffinityPropagationTest {
  private GiraphConfiguration conf;

  @Before
  public void initialize() {
    conf = new GiraphConfiguration();
    conf.setComputationClass(AffinityPropagation.class);
    conf.setMasterComputeClass(MasterComputation.class);
    conf.setInt(AffinityPropagation.MAX_ITERATIONS, 100);
    conf.setFloat(AffinityPropagation.DAMPING, 0.9f);
    conf.setVertexOutputFormatClass(APOutputFormat.class);
    conf.setBoolean("giraph.useSuperstepCounters", false);
  }

  @Test
  public void testVertexInput() {
    String[] graph = {
      "1 1 1 5",
      "2 1 1 3",
      "3 5 3 1",
    };
    String[] expected = {
      "1\t3", "2\t3", "3\t3"
    };

    conf.setVertexInputFormatClass(APVertexInputFormatter.class);

    ImmutableList<String> results;
    try {
      results = Ordering.natural().immutableSortedCopy(
          InternalVertexRunner.run(conf, graph, null));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    assertArrayEquals(expected, results.toArray());
  }

  @Test
  public void testEdgeInput() {
    String[] graph = {
      "1 1 1",
      "1 2 1",
      "1 3 5",
      "2 1 1",
      "2 2 1",
      "2 3 3",
      "3 1 4",
      "3 2 3",
      "3 3 1",
    };
    String[] expected = {
      "1\t3", "2\t3", "3\t3"
    };

    conf.setEdgeInputFormatClass(APEdgeInputFormatter.class);

    ImmutableList<String> results;
    try {
      results = Ordering.natural().immutableSortedCopy(
          InternalVertexRunner.run(conf, null, graph));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    assertArrayEquals(expected, results.toArray());
  }

  @Test
  public void testSparse() {
    String[] graph = {
      "1 1 1",
      "1 2 1",
      "1 3 5",
      "2 1 1",
      "2 2 1",
      "3 1 4",
      "3 3 1",
    };
    String[] expected = {
        "1\t3", "2\t2", "3\t3"
    };

    conf.setEdgeInputFormatClass(APEdgeInputFormatter.class);

    ImmutableList<String> results;
    try {
      results = Ordering.natural().immutableSortedCopy(
          InternalVertexRunner.run(conf, null, graph));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    assertArrayEquals(expected, results.toArray());
  }

  @Test
  public void testToyProblem() throws IOException {
    String[] expected = {
        "1\t3", "2\t3", "3\t3", "4\t3", "5\t3", "6\t3", "7\t7",
        "8\t7", "9\t7", "10\t7", "11\t3", "12\t7", "13\t3", "14\t7",
        "15\t7", "16\t20", "17\t20", "18\t20", "19\t20", "20\t20",
        "21\t20", "22\t3", "23\t20", "24\t20", "25\t7",
    };
    Arrays.sort(expected, Ordering.natural());

    List<String> lines = Resources.readLines(
        Resources.getResource(getClass(), "toyProblem.txt"),
        StandardCharsets.UTF_8);
    String[] graph = lines.toArray(new String[0]);

    conf.setEdgeInputFormatClass(APEdgeInputFormatter.class);

    ImmutableList<String> results;
    try {
      results = Ordering.natural().immutableSortedCopy(
          InternalVertexRunner.run(conf, null, graph));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    assertArrayEquals(expected, results.toArray());
  }

}
