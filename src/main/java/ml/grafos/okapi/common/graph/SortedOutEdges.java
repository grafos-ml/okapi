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
package ml.grafos.okapi.common.graph;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;

import org.apache.giraph.edge.ConfigurableOutEdges;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.MapMutableEdge;
import org.apache.giraph.edge.MutableEdge;
import org.apache.giraph.edge.MutableOutEdges;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.edge.StrictRandomAccessOutEdges;
import org.apache.giraph.utils.EdgeIterables;
import org.apache.hadoop.io.WritableComparable;
import org.weakref.jmx.com.google.common.collect.Sets;

import com.google.common.collect.Maps;

/**
 * {@link OutEdges} implementation backed by a {@link HashMap}
 * with entries sorted by value.
 * Useful when the edges are accessed in value order. 
 *
 * @param <I> Vertex id
 * @param <E> Edge value
 */
@SuppressWarnings("rawtypes")
public class SortedOutEdges<I extends WritableComparable, E extends WritableComparable>
	extends ConfigurableOutEdges<I, E>
	implements StrictRandomAccessOutEdges<I, E>, MutableOutEdges<I, E> {
	
	 /** Map from target vertex id to edge value. */
	  private HashMap<I, E> edgeMap;

	  @Override
	  public void initialize(Iterable<Edge<I, E>> edges) {
	    EdgeIterables.initialize(this, edges);
	  }

	  @Override
	  public void initialize(int capacity) {
	    edgeMap = Maps.newHashMapWithExpectedSize(capacity);
	  }

	  @Override
	  public void initialize() {
	    edgeMap = Maps.newHashMap();
	  }

	  @Override
	  public void add(Edge<I, E> edge) {
	    edgeMap.put(edge.getTargetVertexId(), edge.getValue());
	  }

	  @Override
	  public void remove(I targetVertexId) {
	    edgeMap.remove(targetVertexId);
	  }

	  @Override
	  public E getEdgeValue(I targetVertexId) {
	    return edgeMap.get(targetVertexId);
	  }

	  @Override
	  public void setEdgeValue(I targetVertexId, E edgeValue) {
	    if (edgeMap.containsKey(targetVertexId)) {
	      edgeMap.put(targetVertexId, edgeValue);
	    }
	  }

	  @Override
	  public int size() {
	    return edgeMap.size();
	  }

	  @SuppressWarnings("unchecked")
	@Override
	  public Iterator<Edge<I, E>> iterator() {
	    // Returns an iterator that reuses objects.
	    // The downcast is fine because all concrete Edge implementations are
	    // mutable, but we only expose the mutation functionality when appropriate.
	    return (Iterator) mutableIterator();
	  }

	  @Override
	  public Iterator<MutableEdge<I, E>> mutableIterator() {
	    return new Iterator<MutableEdge<I, E>>() {
	      /** Wrapped map iterator. */
	      @SuppressWarnings("unchecked")
		private Iterator<Map.Entry<I, E>> mapIterator =
	    		  entriesSortedByValues(edgeMap).iterator();
	      /** Representative edge object. */
	      private MapMutableEdge<I, E> representativeEdge =
	          new MapMutableEdge<I, E>();

	      @Override
	      public boolean hasNext() {
	        return mapIterator.hasNext();
	      }

	      @Override
	      public MutableEdge<I, E> next() {
	        representativeEdge.setEntry(mapIterator.next());
	        return representativeEdge;
	      }

	      @Override
	      public void remove() {
	        mapIterator.remove();
	      }
	    };
	  }

	  @Override
	  public void write(DataOutput out) throws IOException {
	    out.writeInt(edgeMap.size());
	    for (Map.Entry<I, E> entry : edgeMap.entrySet()) {
	      entry.getKey().write(out);
	      entry.getValue().write(out);
	    }
	  }

	  @Override
	  public void readFields(DataInput in) throws IOException {
	    int numEdges = in.readInt();
	    initialize(numEdges);
	    for (int i = 0; i < numEdges; ++i) {
	      I targetVertexId = getConf().createVertexId();
	      targetVertexId.readFields(in);
	      E edgeValue = getConf().createEdgeValue();
	      edgeValue.readFields(in);
	      edgeMap.put(targetVertexId, edgeValue);
	    }
	  }
	  
	  static <K extends Comparable,V extends Comparable<? super V>>
	  SortedSet<Entry<K,V>> entriesSortedByValues(Map<K,V> map) {
	      SortedSet<Entry<K,V>> sortedEntries = Sets.newTreeSet(new EdgeValueComparator<K, V>());
	      sortedEntries.addAll(map.entrySet());
	      return sortedEntries;
	  }
}