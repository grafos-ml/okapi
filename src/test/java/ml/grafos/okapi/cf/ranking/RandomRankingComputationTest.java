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
import org.junit.Before;
import org.junit.Test;

/**
 * @author User: linas
 *         Date: 11/25/13
 */
public class RandomRankingComputationTest {

    static List<String> res;
    static List<String> res2;

    @Before
    public void setUp() throws Exception {
        String[] graph = {
                "1 1 1",
                "2 1 1"
        };


        GiraphConfiguration conf = new GiraphConfiguration();
        conf.setComputationClass(RandomRankingComputation.class);
        conf.setEdgeInputFormatClass(CfLongIdFloatTextInputFormat.class);
        conf.setVertexOutputFormatClass(IdWithValueTextOutputFormat.class);

        RandomRankingComputation.setDim(2);
        Iterable<String> results = InternalVertexRunner.run(conf, null, graph);
        res = new LinkedList<String>();
        for (String string : results) {
            res.add(string);
        }

        RandomRankingComputation.setDim(10);
        results = InternalVertexRunner.run(conf, null, graph);
        res2 = new LinkedList<String>();
        for (String string : results) {
            res2.add(string);
        }

    }

    @Test
    public void testFullComputation() throws Exception {
        Assert.assertEquals(3, res.size());
        Assert.assertTrue(res.get(0).startsWith("1 0\t["));
        Assert.assertTrue(res.get(1).startsWith("2 0\t["));
        Assert.assertTrue(res.get(2).startsWith("1 1\t["));
    }

    @Test
    public void testDimensionality() throws Exception {
        Assert.assertEquals(2, res.get(0).split(";").length);
        Assert.assertEquals(2, res.get(1).split(";").length);
        Assert.assertEquals(2, res.get(2).split(";").length);

        Assert.assertEquals(10, res2.get(0).split(";").length);
        Assert.assertEquals(10, res2.get(1).split(";").length);
        Assert.assertEquals(10, res2.get(2).split(";").length);
    }
}
