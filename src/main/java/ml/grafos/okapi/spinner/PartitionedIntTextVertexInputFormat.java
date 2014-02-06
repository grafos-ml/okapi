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
package ml.grafos.okapi.spinner;

import java.util.List;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.io.formats.AdjacencyListTextVertexInputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Lists;

public class PartitionedIntTextVertexInputFormat
		extends
		AdjacencyListTextVertexInputFormat<PartitionedLongWritable, DoubleWritable, NullWritable> {

	@Override
	public AdjacencyListTextVertexReader createVertexReader(InputSplit split,
			TaskAttemptContext context) {
		return new IntIntVertexReader();
	}

	public class IntIntVertexReader
			extends
			AdjacencyListTextVertexInputFormat<PartitionedLongWritable, DoubleWritable, NullWritable>.AdjacencyListTextVertexReader {

		@Override
		public PartitionedLongWritable decodeId(String s) {
			return new PartitionedLongWritable(s);
		}

		@Override
		public DoubleWritable decodeValue(String s) {
			return new DoubleWritable(1);
		}

		@Override
		public Edge<PartitionedLongWritable, NullWritable> decodeEdge(
				String id, String value) {
			return EdgeFactory.create(decodeId(id), NullWritable.get());
		}

		@Override
		protected Iterable<Edge<PartitionedLongWritable, NullWritable>> getEdges(
				String[] values) {
			List<Edge<PartitionedLongWritable, NullWritable>> edges = Lists
					.newLinkedList();
			for (int i = 1; i < values.length; i++) {
				edges.add(decodeEdge(values[i], null));
			}
			return edges;
		}

		protected String[] preprocessLine(Text line) {
			return line.toString().trim().split(" ");
		}
	}
}
