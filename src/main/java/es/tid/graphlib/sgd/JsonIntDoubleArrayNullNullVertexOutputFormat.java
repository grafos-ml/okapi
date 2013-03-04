package es.tid.graphlib.sgd;

//import org.apache.giraph.graph.Edge;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.giraph.vertex.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.JSONArray;
import org.json.JSONException;

import java.io.IOException;

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


/**
 * VertexOutputFormat that supports JSON encoded vertices featuring
 * <code>double</code> values and <code>float</code> out-edge weights
 */
public class JsonIntDoubleArrayNullNullVertexOutputFormat extends
  TextVertexOutputFormat<IntWritable, DoubleArrayListWritable,
  IntWritable> {
	@Override
	public TextVertexWriter createVertexWriter(TaskAttemptContext context) {
		return new JsonIntDoubleArrayNullNullVertexWriter();
    }
	/**
	 * VertexWriter that supports vertices with 
	 * <code>DoubleArrayListWritable</code> values 
	 * <code>Int</code> out-edge weights.
	 */
	private class JsonIntDoubleArrayNullNullVertexWriter extends
	TextVertexWriterToEachLine{
		public Text convertVertexToLine(
				Vertex<IntWritable, DoubleArrayListWritable, IntWritable, ?> vertex) 
						throws IOException {
			boolean flag = getContext().getConfiguration().getBoolean("sgd.printerr", false);
			System.out.println("flag=" + flag);
			JSONArray jsonVertex = new JSONArray();
			jsonVertex.put(vertex.getId().get());
			jsonVertex.put(vertex.getValue());
			if (flag == true){
				try {
					jsonVertex.put(((SGD)vertex).e);
				} catch (JSONException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				//System.out.println("e=" + (SGD)vertex.e);
			}
			return new Text(jsonVertex.toString());
		}
	}
}