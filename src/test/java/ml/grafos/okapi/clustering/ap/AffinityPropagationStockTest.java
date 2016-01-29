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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

public class AffinityPropagationStockTest {
  private GiraphConfiguration conf;

  @Before
  public void initialize() {
    conf = new GiraphConfiguration();
    conf.setComputationClass(AffinityPropagation.class);
    conf.setMasterComputeClass(MasterComputation.class);
    conf.setInt(AffinityPropagation.MAX_ITERATIONS, 100);
    conf.setFloat(AffinityPropagation.DAMPING, 0.5f);
    conf.setVertexOutputFormatClass(APOutputFormat.class);
    conf.setBoolean("giraph.useSuperstepCounters", false);
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
        Resources.getResource(getClass(), "stockdailypropcovariance.txt"),
        StandardCharsets.UTF_8);
    String[] graph = lines.toArray(new String[0]);

    conf.setVertexInputFormatClass(APVertexInputFormatter.class);

    ImmutableList<String> results;
    try {
      results = Ordering.natural().immutableSortedCopy(
              InternalVertexRunner.run(conf, graph, null));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    int[] cluster = new int[results.size()];
    for(String res: results){
        String[] parts = res.split("\t");
        cluster[Integer.parseInt(parts[0]) - 1] = Integer.parseInt(parts[1]);
    }

      File file = new File("clustersdaily.txt");

      FileWriter fw = new FileWriter(file.getAbsoluteFile());
      BufferedWriter bw = new BufferedWriter(fw);
      for (int i = 0; i < cluster.length; i++) {
          bw.write(String.valueOf(cluster[i]));
          bw.newLine();
      }

      bw.close();

    assertArrayEquals(expected, results.toArray());
  }

}