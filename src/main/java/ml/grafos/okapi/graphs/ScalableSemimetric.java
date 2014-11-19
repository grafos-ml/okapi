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
package ml.grafos.okapi.graphs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.AbstractComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

/**
 * This is a set of computation classes used to find semi-metric edges in the
 * triangles of a graph. If vertices A, B, C form a triangle, then edge AB is
 * semi-metric if D(A,B) > D(A,C)+D(C,B).
 * 
 * 
 * This implementation divides the algorithm into several megasteps
 * which contain the three supersteps of the main computation.
 * In each megastep, some of the vertices of the graph execute the 
 * algorithm, while the rest are idle (but still active).
 * The algorithm finishes, when all vertices have executed the computation.
 * In the case of semi-metric edge removal, this model is possible, because
 * messages are neither aggregated nor combined. 
 * 
 * This implementation assumes numeric vertex ids.
 *  
 * 
 * 
 * You can run this algorithm by executing the command:
 * 
 * 
 * hadoop jar $OKAPI_JAR org.apache.giraph.GiraphRunner \
 *   ml.grafos.okapi.graphs.ScalableSemimetric\$PropagateId  \
 *   -mc  ml.grafos.okapi.graphs.ScalableSemimetric\$SemimetricMasterCompute  \
 *   -eif ml.grafos.okapi.io.formats.LongDoubleBooleanEdgeInputFormat  \
 *   -eip $INPUT_EDGES \
 *   -eof org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat \
 *   -op $OUTPUT \
 *   -w $WORKERS \
 *   -ca giraph.outEdgesClass=org.apache.giraph.edge.HashMapEdges
 *  
 *  You can set the number of megasteps by setting the configuration option
 *  semimetric.megasteps.
 * 
 * 
 */
public class ScalableSemimetric  {
  
  /** Indicates in how many megasteps to divide the computation. */
  public static final String NUMBER_OF_MEGASTEPS = "semimetric.megasteps";
  
  /** Default value of megasteps */
  public static final int NUMBER_OF_MEGASTEPS_DEFAULT = 2;

  /**
   * This class implements the first stage, which propagates the ID of a vertex
   * to all neighbors with higher ID.
   *
   */
  public static class PropagateId extends AbstractComputation<LongWritable,
  NullWritable, DoubleBooleanPair, LongWritable, LongWritable> {

	  int megasteps;
	  
	  @Override
	  public void preSuperstep() {
		  megasteps = getContext().getConfiguration()
				  .getInt(NUMBER_OF_MEGASTEPS, NUMBER_OF_MEGASTEPS_DEFAULT);
	  }
	  
    @Override
    public void compute(Vertex<LongWritable, NullWritable,DoubleBooleanPair> vertex, 
        Iterable<LongWritable> messages) throws IOException {
    	
    	final long vertexId = vertex.getId().get();
    	final long superstep = getSuperstep();
    	
    	if (((vertexId % megasteps) * 3) == superstep) {
    		for (Edge<LongWritable, DoubleBooleanPair> edge: vertex.getEdges()) {
    	        if (edge.getTargetVertexId().compareTo(vertex.getId()) > 0) {
    	          sendMessage(edge.getTargetVertexId(), vertex.getId());
    	        }
    	      }
    	}
    	
    	// handle set-semimetric-label messages
    	for (LongWritable trg: messages) {
    		if (vertex.getEdgeValue(trg) != null) {
    			vertex.setEdgeValue(trg, vertex.getEdgeValue(trg).setSemimetric(true));
    		}
    	}
    }
  } 

  /**
   * This class implements the second phase of the algorithm that finds all
   * unique triangles (not just counting) them. The difference with the 
   * ForwardId implementation, is that it sends a pair of IDs: the ID included
   * in the message sent from the first phase, and the ID of the current vertex.
   *
   */
  public static class ForwardEdge extends AbstractComputation<LongWritable, 
  Writable, DoubleBooleanPair, LongWritable, SimpleEdge> {
    @Override
    public void compute(Vertex<LongWritable, Writable, DoubleBooleanPair> vertex, 
        Iterable<LongWritable> messages) throws IOException {
    	
	      for (LongWritable msg : messages) {
	        assert(msg.compareTo(vertex.getId())<0); // This can never happen
	
	        double weight = vertex.getEdgeValue(msg).getWeight();
	
	        // This means there is an edge:
	        // 1) FROM vertex with ID=msg.get()
	        // 2) TO vertex with ID=vertex.getId().get()
	        // 3) with the specified weight.
	        SimpleEdge t = new SimpleEdge(msg.get(), vertex.getId().get(), weight);
	
	        for (Edge<LongWritable, DoubleBooleanPair> edge: vertex.getEdges()) {
	          if (vertex.getId().compareTo(edge.getTargetVertexId()) < 0) {
	            sendMessage(edge.getTargetVertexId(), t);
	          }
	        } 
	      }
    }
  }

  /**
   * This class implements the third phase of the algorithm that detects whether
   * a triangle has closed and whether there is a semi-metric edge in this
   * triangle.
   */
  @SuppressWarnings("rawtypes")
public static class FindSemimetricEdges extends AbstractComputation<LongWritable, 
    Writable, DoubleBooleanPair, SimpleEdge, WritableComparable> {
	  
    @Override
    public void compute(Vertex<LongWritable, Writable, DoubleBooleanPair> vertex, 
        Iterable<SimpleEdge> messages) 
            throws IOException {

	      for (SimpleEdge msg : messages) {
	        // If this vertex has a neighbor with this ID, then this means it
	        // participates in a triangle.
	        
	        // We are in vertex A=vertex.getId().
	        // We received a message from vertex B=msg.getId2() that tells that 
	        // there is an edge between vertex B and vertex C=msg.getId1(), with
	        // weight W_BC=msg.getWeight();
	
	        LongWritable id1 = new LongWritable(msg.getId1());
	        LongWritable id2 = new LongWritable(msg.getId2());
	
	        // First we are going to check whether A,B and C are a triangle
	        if (vertex.getEdgeValue(id1)!=null) {
	          // If they are a triangle, we check for semimetricity. We check 
	          // whether one of the following is true:
	          // 1) W_AB+W_AC < W_BC => BC is semimetric
	          // 2) W_AB+W_BC < W_AC => AC is semimetric
	          // 3) W_BC+W_AC < W_AB => AB is semimetric
	          
	          double weight_ab = vertex.getEdgeValue(id2).getWeight();
	          double weight_ac = vertex.getEdgeValue(id1).getWeight();
	          double weight_bc = msg.getWeight(); 
	          
	          if (weight_ab+weight_ac < weight_bc) {
	        	  sendMessage(id1, id2);
	        	  sendMessage(id2, id1);
	          } else if (weight_ab+weight_bc < weight_ac) {
	            	vertex.setEdgeValue(id1, vertex.getEdgeValue(id1).setSemimetric(true));
	            	sendMessage(id1, vertex.getId());
	            
	          } else if (weight_bc+weight_ac < weight_ab) {
	            	vertex.setEdgeValue(id2, vertex.getEdgeValue(id2).setSemimetric(true));
	            	sendMessage(id2, vertex.getId());
	          }
	        }
    	}
     }
  }

  /**
   * 
   * Request remove all edges that have been labeled as semi-metric.  
   * 
   *
   */
  public static class Finalize extends AbstractComputation<LongWritable, 
  NullWritable, DoubleBooleanPair, LongWritable, Writable> {

    @Override
    public void compute(Vertex<LongWritable, NullWritable, DoubleBooleanPair> vertex,
        Iterable<LongWritable> messages) throws IOException {
    	
    	// handle set-semimetric-label messages
    	for (LongWritable trg: messages) {
    		if (vertex.getEdgeValue(trg) != null) {
    			vertex.setEdgeValue(trg, vertex.getEdgeValue(trg).setSemimetric(true));
    		}
    	}
    	
    	for (Edge<LongWritable, DoubleBooleanPair> edge : vertex.getEdges()) {
    		if (edge.getValue().isSemimetric()) {
    			// remove edge
    			removeEdgesRequest(vertex.getId(), edge.getTargetVertexId());	
    		}
    	}
    }
  }
  
  public static class RemoveEdges extends AbstractComputation<LongWritable, 
  NullWritable, DoubleBooleanPair, Writable, Writable> {

    @Override
    public void compute(Vertex<LongWritable, NullWritable, DoubleBooleanPair> vertex,
        Iterable<Writable> messages) throws IOException {
      vertex.voteToHalt();
    }
  }
  

  /**
   * Represents an undirected edge with a symmetric weight.
   *
   */
  public static class SimpleEdge implements Writable {
    long id1;
    long id2;
    double weight;

    public SimpleEdge() {}

    public SimpleEdge(long id1, long id2, double weight) {
      this.id1 = id1;
      this.id2 = id2;
      this.weight = weight;
    }

    public long getId1() { return id1; }
    public long getId2() { return id2; }
    public double getWeight() { return weight; }

    @Override
    public void readFields(DataInput input) throws IOException {
      id1 = input.readLong();
      id2 = input.readLong();
      weight = input.readDouble();
    }

    @Override
    public void write(DataOutput output) throws IOException {
      output.writeLong(id1);
      output.writeLong(id2);
      output.writeDouble(weight);
    }
    
    @Override
    public String toString() {
      return id1+" "+id2+" "+weight;
    }
  }
  
  /**
   * Represents the edge value type.
   * It contains the edge weight and a boolean
   * label that indicates whether the edge is semi-metric.
   *
   */
  @SuppressWarnings("rawtypes")
  public static class DoubleBooleanPair implements WritableComparable {
      double weight;
      boolean semimetric = false;

      public DoubleBooleanPair() {}

      public DoubleBooleanPair(double weight, boolean metric) {
        this.weight = weight;
        this.semimetric = metric;
      }

      public double getWeight() { return weight; }
      public boolean isSemimetric() { return semimetric; }
      
      public DoubleBooleanPair setSemimetric(boolean value) {
      	this.semimetric = value; 
      	return this;
      }

      @Override
      public void readFields(DataInput input) throws IOException {
        weight = input.readDouble();
        semimetric = input.readBoolean();
      }

      @Override
      public void write(DataOutput output) throws IOException {
        output.writeDouble(weight);
        output.writeBoolean(semimetric);
      }
      
      @Override
      public String toString() {
        return weight + "";
      }

  	@Override
  	public int compareTo(Object other) {
  		DoubleBooleanPair otherPair = (DoubleBooleanPair) other;
  		if (this.getWeight() < otherPair.getWeight()) {
  			return -1;
  		}
  		else if (this.getWeight() > otherPair.getWeight()) {
  			return 1;
  		}
  		else {
  			return 0;
  		}
  	}
    }
  
  /**
   * Use this MasterCompute implementation to find the semi-metric edges.
   *
   */
  public static class SemimetricMasterCompute extends DefaultMasterCompute {
    
    @Override
    public void compute() {

      int subSupersteps = getContext().getConfiguration()
			  .getInt(NUMBER_OF_MEGASTEPS, NUMBER_OF_MEGASTEPS_DEFAULT);

      long superstep = getSuperstep(); 
      if (superstep < subSupersteps*3) {
	      if ((superstep%3)==0) {
	        setComputation(PropagateId.class);
	        setIncomingMessage(LongWritable.class);
	        setOutgoingMessage(LongWritable.class);
	      } else if ((superstep%3)==1) {
	        setComputation(ForwardEdge.class);
	        setIncomingMessage(LongWritable.class);
	        setOutgoingMessage(SimpleEdge.class);
	      } else if ((superstep%3)==2){
	        setComputation(FindSemimetricEdges.class);
	        setIncomingMessage(SimpleEdge.class);
	        setOutgoingMessage(LongWritable.class);
	      }  
      } else if (superstep == subSupersteps*3) {
  	         // remove the semi-metric edges
  	          setComputation(Finalize.class);
  	          setIncomingMessage(LongWritable.class);
  	          setOutgoingMessage(LongWritable.class); 
        }
      else if (superstep == (subSupersteps*3) + 1) {
	         // remove the semi-metric edges
	          setComputation(RemoveEdges.class);
	          setIncomingMessage(LongWritable.class);
	          setOutgoingMessage(LongWritable.class); 
      }
      else {
    	  haltComputation();
      }
    }
  }
}