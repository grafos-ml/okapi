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
package ml.grafos.okapi.common.graphs;

import static org.apache.giraph.graph.TestVertexAndEdges.instantiateOutEdges;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import ml.grafos.okapi.common.graph.SortedOutEdges;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.edge.OutEdges;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

import com.google.common.collect.Lists;

@SuppressWarnings("unchecked")
public class TestSortedOutEdges {
	
	private static final float E = 0.0001f;

	  @Test
	  public void testParallelEdges() {
	    OutEdges<LongWritable, DoubleWritable> edges = instantiateOutEdges(SortedOutEdges.class);
	
	    // Initial edges list contains parallel edges.
	    List<Edge<LongWritable, DoubleWritable>> initialEdges = Lists.newArrayList(
	        EdgeFactory.create(new LongWritable(1), new DoubleWritable(1)),
	        EdgeFactory.create(new LongWritable(2), new DoubleWritable(2)),
	        EdgeFactory.create(new LongWritable(3), new DoubleWritable(3)),
	        EdgeFactory.create(new LongWritable(2), new DoubleWritable(20)));
	
	    edges.initialize(initialEdges);
	
	    // Only one of the two parallel edges should be left.
	    assertEquals(3, edges.size());
	
	    // Adding a parallel edge shouldn't change the number of edges.
	    edges.add(EdgeFactory.create(new LongWritable(3), new DoubleWritable(30)));
	    assertEquals(3, edges.size());
	  }
	  
	  @Test
	  public void testSortOrder() {
	    OutEdges<LongWritable, DoubleWritable> edges = instantiateOutEdges(SortedOutEdges.class);

	    List<Edge<LongWritable, DoubleWritable>> initialEdges = Lists.newArrayList(
	        EdgeFactory.create(new LongWritable(1), new DoubleWritable(3.0)),
	        EdgeFactory.create(new LongWritable(2), new DoubleWritable(4.5)),
	        EdgeFactory.create(new LongWritable(3), new DoubleWritable(1.0)),
	        EdgeFactory.create(new LongWritable(4), new DoubleWritable(-2.0)));
	
	    edges.initialize(initialEdges);
	
	    // edges should be ordered by value
	    List<LongWritable> ids = new ArrayList<LongWritable>();
	    for (Edge<LongWritable, DoubleWritable> edge : edges) {
	    	ids.add(edge.getTargetVertexId());
	    }
	    
	    assertEquals(4, ids.get(0).get());
	    assertEquals(3, ids.get(1).get());
	    assertEquals(1, ids.get(2).get());
	    assertEquals(2, ids.get(3).get());
	
	  }
	  
	  @Test
	  public void testGetValue() {
	    OutEdges<LongWritable, DoubleWritable> edges = instantiateOutEdges(SortedOutEdges.class);

	    List<Edge<LongWritable, DoubleWritable>> initialEdges = Lists.newArrayList(
	        EdgeFactory.create(new LongWritable(1), new DoubleWritable(1)),
	        EdgeFactory.create(new LongWritable(2), new DoubleWritable(2)),
	        EdgeFactory.create(new LongWritable(3), new DoubleWritable(3)),
	        EdgeFactory.create(new LongWritable(4), new DoubleWritable(4)));
	
	    edges.initialize(initialEdges);
	
	    // test edge.getValue
	    List<DoubleWritable> values = new ArrayList<DoubleWritable>();
	    for (Edge<LongWritable, DoubleWritable> edge : edges) {
	    	values.add(edge.getValue());
	    }
	    
	    assertEquals(1, values.get(0).get(), E);
	    assertEquals(2, values.get(1).get(), E);
	    assertEquals(3, values.get(2).get(), E);
	    assertEquals(4, values.get(3).get(), E);
	    
	  }
	  
	  @Test
	  public void testAddRemove() {
	    OutEdges<LongWritable, DoubleWritable> edges = instantiateOutEdges(SortedOutEdges.class);

	    List<Edge<LongWritable, DoubleWritable>> initialEdges = Lists.newArrayList(
	        EdgeFactory.create(new LongWritable(1), new DoubleWritable(1)),
	        EdgeFactory.create(new LongWritable(2), new DoubleWritable(2)),
	        EdgeFactory.create(new LongWritable(3), new DoubleWritable(3)),
	        EdgeFactory.create(new LongWritable(4), new DoubleWritable(4)));
	
	    edges.initialize(initialEdges);
	    
	    // test add
	    edges.add(EdgeFactory.create(new LongWritable(5), new DoubleWritable(5)));
	    assertEquals(5, edges.size());
	
	    // test remove
	    LongWritable vertexID = edges.iterator().next().getTargetVertexId();
	    edges.remove(vertexID);
	    assertEquals(4, edges.size());
	    	    
	  }
	  
	  @Test
		public void testSerialize() {
		  OutEdges<LongWritable, DoubleWritable> edges = instantiateOutEdges(SortedOutEdges.class);

		    List<Edge<LongWritable, DoubleWritable>> initialEdges = Lists.newArrayList(
		        EdgeFactory.create(new LongWritable(1), new DoubleWritable(1)),
		        EdgeFactory.create(new LongWritable(2), new DoubleWritable(2)),
		        EdgeFactory.create(new LongWritable(3), new DoubleWritable(3)),
		        EdgeFactory.create(new LongWritable(4), new DoubleWritable(4)));
		
		    edges.initialize(initialEdges);
			
			// Serialize from
			byte[] data = WritableUtils.writeToByteArray(edges, edges);
			
			// De-serialize to
			OutEdges<LongWritable, DoubleWritable> to1 = instantiateOutEdges(SortedOutEdges.class);
			OutEdges<LongWritable, DoubleWritable> to2 = instantiateOutEdges(SortedOutEdges.class);
			
			WritableUtils.readFieldsFromByteArray(data, to1, to2);
			
			// all coordinates should be equal
			List<Edge<LongWritable, DoubleWritable>> to1Edges = new ArrayList<Edge<LongWritable, DoubleWritable>>();
			for (Edge<LongWritable, DoubleWritable> e : to1) {
				to1Edges.add(e);
			}
			
			List<Edge<LongWritable, DoubleWritable>> to2Edges = new ArrayList<Edge<LongWritable, DoubleWritable>>();
			for (Edge<LongWritable, DoubleWritable> e : to2) {
				to2Edges.add(e);
			}
			
			assertEquals(edges.size(), to1.size());
			assertEquals(edges.size(), to2.size());
			
			for (int i=0; i>edges.size(); i++) {
				assertEquals(to1Edges.get(i).getTargetVertexId(), to2Edges.get(i).getTargetVertexId());
				assertEquals(to1Edges.get(i).getValue(), to2Edges.get(i).getValue());
			}
		}
}