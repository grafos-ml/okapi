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
package ml.grafos.okapi.cf.ranking;

import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;
import ml.grafos.okapi.cf.CfLongIdFloatTextInputFormat;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.io.formats.IdWithValueTextOutputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.junit.Test;

public class PopularityRankingComputationTest {

	@Test
	public void testFullComputation() throws Exception {
		String[] graph = { 
				"1 1 1",
				"2 1 1"
		};

		GiraphConfiguration conf = new GiraphConfiguration();
		conf.setComputationClass(PopularityRankingComputation.class);
        conf.setEdgeInputFormatClass(CfLongIdFloatTextInputFormat.class);
        conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);

        Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
		List<String> res = new LinkedList<String>();
		for (String string : results) {
			res.add(string);
            //System.out.println(string);
        }
		Assert.assertEquals(3, res.size());
        Assert.assertTrue(res.contains("1 0\t[1.000000]"));
        Assert.assertTrue(res.contains("2 0\t[1.000000]"));
        Assert.assertTrue(res.contains("1 1\t[2.000000]"));
	}
}
