/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package es.tid.graphlib.sgd;

import org.apache.giraph.graph.DefaultEdge;
import org.apache.giraph.graph.Edge;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.giraph.vertex.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

/**
  * VertexInputFormat that features: 
  * <code>int</code> vertex ID,
  * <code>double</code> vertex values,
  * <code>int</code> edge weights, and 
  * <code>int</code> message types,
  *  specified in JSON format.
  */
public class JsonIntDoubleArrayIntIntVertexInputFormat extends
  TextVertexInputFormat<IntWritable, DoubleArrayListWritable,
  IntWritable, IntWritable> {

  @Override
  public TextVertexReader createVertexReader(InputSplit split,
      TaskAttemptContext context) {
    return new JsonIntDoubleIntIntVertexReader();
  }

 /**
  * VertexReader that features <code>double</code> vertex
  * values and <code>int</code> out-edge weights. The
  * files should be in the following JSON format:
  * JSONArray(<vertex id>, 
  * 	JSONArray(<vertex value x>, <vertex value y>),
  * 	JSONArray(JSONArray(<dest vertex id>, <edge value>), ...))
  * 
  * Here is an example with vertex id 1, vertex values 4.3 and 2.1,
  * and two edges.
  * First edge has a destination vertex 2, edge value 2.1.
  * Second edge has a destination vertex 3, edge value 0.7.
  * [1,[4.3,2.1],[[2,2.1],[3,0.7]]]
  */
  class JsonIntDoubleIntIntVertexReader extends
    TextVertexReaderFromEachLineProcessedHandlingExceptions<JSONArray,
    JSONException> {

    @Override
    protected JSONArray preprocessLine(Text line) throws JSONException {
      return new JSONArray(line.toString());
    }

    @Override
    protected IntWritable getId(JSONArray jsonVertex) throws JSONException,
              IOException {
      return new IntWritable(jsonVertex.getInt(0));
    }
    /*
    @Override
    protected DoubleWritable getValue(JSONArray jsonVertex) throws
      JSONException, IOException {
      return new DoubleWritable(jsonVertex.getDouble(1));
    }
     */
/*    protected DoubleWritable getValue(JSONArray jsonVertex) throws
    JSONException, IOException {
    	JSONArray jsonValueArray = jsonVertex.getJSONArray(2);
    	
    }
*/
    /*
    protected Value<DoubleWritable, DoubleWritable> getValue(
    		JSONArray jsonVertex) throws JSONException, IOException {
    	JSONArray jsonValue = jsonVertex.getJSONArray(2);
    	Value<DoubleWritable, DoubleWritable> value =
    			new DefaultValue<DoubleWritable, DoubleWritable>(
    					new DoubleWritable(jsonValue.getDouble(0)),
    					new DoubleWritable(jsonValue.getDouble(1)));
    	return value;
    }
    */
    protected DoubleArrayListWritable getValue(
    		JSONArray jsonVertex) throws JSONException, IOException {
    	// Create a JSON array for the second field of the line
    	JSONArray jsonValueArray = jsonVertex.getJSONArray(1);
    	// create an object 
    	DoubleArrayListWritable values = new DoubleArrayListWritable();
    	for (int i=0; i < jsonValueArray.length(); ++i){
    		values.add(new DoubleWritable(jsonValueArray.getDouble(i)));
    	}
    	return values;
    }
    
    @Override
    protected Iterable<Edge<IntWritable, IntWritable>> getEdges(
        JSONArray jsonVertex) throws JSONException, IOException {
      JSONArray jsonEdgeArray = jsonVertex.getJSONArray(2);
      List<Edge<IntWritable, IntWritable>> edges =
          Lists.newArrayListWithCapacity(jsonEdgeArray.length());
      for (int i = 0; i < jsonEdgeArray.length(); ++i) {
        JSONArray jsonEdge = jsonEdgeArray.getJSONArray(i);
        edges.add(new DefaultEdge<IntWritable, IntWritable>(
            new IntWritable(jsonEdge.getInt(0)),
            new IntWritable(jsonEdge.getInt(1))));
      }
      return edges;
    }

    @Override
    protected Vertex<IntWritable, DoubleArrayListWritable, IntWritable,
              IntWritable> handleException(Text line, JSONArray jsonVertex,
                  JSONException e) {
      throw new IllegalArgumentException(
          "Couldn't get vertex from line " + line, e);
    }

  }
}
