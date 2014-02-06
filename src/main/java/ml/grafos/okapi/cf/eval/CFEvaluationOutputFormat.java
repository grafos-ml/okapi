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
package ml.grafos.okapi.cf.eval;

import java.io.IOException;

import ml.grafos.okapi.cf.CfLongId;
import ml.grafos.okapi.common.jblas.FloatMatrixWritable;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexOutputFormat;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CFEvaluationOutputFormat extends
	TextVertexOutputFormat<CfLongId, FloatMatrixWritable, BooleanWritable> {

	CfLongId outputEdge = new CfLongId((byte)-1, 0);
	
	@Override
	public TextVertexWriter createVertexWriter(
			TaskAttemptContext context) throws IOException,
			InterruptedException {
		
		TextVertexWriterToEachLine tvwt = new TextVertexWriterToEachLine() {

			@Override
			protected Text convertVertexToLine(
					Vertex<CfLongId, FloatMatrixWritable, BooleanWritable> vertex)
					throws IOException {
				if (outputEdge.equals(vertex.getId())){
					return new Text(vertex.getValue().toString());
				}
				return null;
			}
		};
		
		return tvwt;
	}

}
