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
package ml.grafos.okapi.spinner;

import it.unimi.dsi.fastutil.longs.Long2ShortMap;
import it.unimi.dsi.fastutil.longs.Long2ShortOpenHashMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import ml.grafos.okapi.spinner.Spinner.EdgeValue;

import org.apache.giraph.edge.ConfigurableOutEdges;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.edge.StrictRandomAccessOutEdges;
import org.apache.giraph.utils.EdgeIterables;
import org.apache.hadoop.io.LongWritable;

public class OpenHashMapEdges extends
		ConfigurableOutEdges<LongWritable, EdgeValue> implements
		StrictRandomAccessOutEdges<LongWritable, EdgeValue> {
	private Long2ShortMap map;
	private EdgeValue repValue = new EdgeValue();

	@Override
	public void initialize(Iterable<Edge<LongWritable, EdgeValue>> edges) {
		EdgeIterables.initialize(this, edges);
	}

	@Override
	public void initialize(int capacity) {
		map = new Long2ShortOpenHashMap(capacity);
	}

	@Override
	public void initialize() {
		map = new Long2ShortOpenHashMap();
	}

	@Override
	public void add(Edge<LongWritable, EdgeValue> edge) {
		map.put(edge.getTargetVertexId().get(), edge.getValue().getPartition());
	}

	@Override
	public void remove(LongWritable targetVertexId) {
		map.remove(targetVertexId.get());
	}

	@Override
	public int size() {
		return map.size();
	}

	@Override
	public Iterator<Edge<LongWritable, EdgeValue>> iterator() {
		return (Iterator) mutableIterator();
	}

	public Iterator<MutableEdge<LongWritable, EdgeValue>> mutableIterator() {
		return new Iterator<MutableEdge<LongWritable, EdgeValue>>() {
			private Iterator<Entry<Long, Short>> it = map.entrySet().iterator();
			private MutableEdge<LongWritable, EdgeValue> repEdge = EdgeFactory
					.createReusable(new LongWritable(), new EdgeValue());

			@Override
			public boolean hasNext() {
				return it.hasNext();
			}

			@Override
			public MutableEdge<LongWritable, EdgeValue> next() {
				Entry<Long, Short> entry = it.next();
				repEdge.getTargetVertexId().set(entry.getKey());
				repEdge.getValue().setPartition(entry.getValue());
				return repEdge;
			}

			@Override
			public void remove() {
			}
		};
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		int numEdges = in.readInt();
		initialize(numEdges);
		for (int i = 0; i < numEdges; i++) {
			long id = in.readLong();
			short v = in.readShort();
			map.put(id, v);
		}
	}

	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeInt(map.size());
		for (Entry<Long, Short> e : map.entrySet()) {
			out.writeLong(e.getKey());
			out.writeShort(e.getValue());
		}
	}

	@Override
	public EdgeValue getEdgeValue(LongWritable targetVertexId) {
		short v = map.get(targetVertexId.get());
		repValue.setPartition(v);
		return repValue;
	}

	@Override
	public void setEdgeValue(LongWritable targetVertexId, EdgeValue edgeValue) {
		map.put(targetVertexId.get(), edgeValue.getPartition());
	}
}
