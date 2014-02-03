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
package ml.grafos.okapi.cf.eval;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RankEvaluationComputationTest {

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	
	public void testFullComputation() throws Exception {
		String[] graph = { 
				"0 -1",
				"1 0	[1;0;0]	-1,-2",
				"2 0	[2;0;0]	-1",
				"-1 1	[1;1;1]",
				"-2 1	[0.5;1;1]",
				"-3 1	[0;0;0]",
				"-4 1	[0;0;0]"};

		GiraphConfiguration conf = new GiraphConfiguration();
		conf.setComputationClass(RankEvaluationComputation.class);
		conf.setVertexInputFormatClass(CfModelInputFormat.class);
		conf.setVertexOutputFormatClass(CFEvaluationOutputFormat.class);
		conf.set("minItemId", "-4");
		conf.set("maxItemId", "-1");
		conf.set("numberSamples", "1");
		conf.set("k", "2");
		Iterable<String> results = InternalVertexRunner.run(conf, graph);

		String line = results.iterator().next();
		line = line.replace("[", "");
		line = line.replace("]", "");
		line = line.replace(";", "");
		assertEquals((1+0.5)/2.0, Double.parseDouble(line), 0.01);
		
	}


	private static Map<Integer, Integer> parseResults(Iterable<String> results) {
		Map<Integer, Integer> values = new HashMap<Integer, Integer>();
		for (String line : results) {
			String[] tokens = line.split("\\s+");
			int id = Integer.valueOf(tokens[0]);
			int value = Integer.valueOf(tokens[1]);
			values.put(id, value);
		}
		return values;
	}
}
