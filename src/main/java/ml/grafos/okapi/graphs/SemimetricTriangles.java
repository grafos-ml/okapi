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
import org.apache.hadoop.io.DoubleWritable;
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
 * You can run this algorithm by executing the command:
 * 
 * <pre>
 * hadoop jar $OKAPI_JAR org.apache.giraph.GiraphRunner \
 *   ml.grafos.okapi.graphs.SemimetricTriangles\$PropagateId  \
 *   -mc  ml.grafos.okapi.graphs.SemimetricTriangles\$SemimetricMasterCompute  \
 *   -eif ml.grafos.okapi.io.formats.LongDoubleTextEdgeInputFormat  \
 *   -eip $INPUT_EDGES \
 *   -eof org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat \
 *   -op $OUTPUT \
 *   -w $WORKERS \
 *   -ca giraph.outEdgesClass=org.apache.giraph.edge.HashMapEdges
 *  </pre>
 * 
 * 
 * @author dl
 */
public class SemimetricTriangles  {
  
  /** Indicates whether semi-metric edges will be removed in the output graph. */
  public static final String REMOVE_EDGES_ENABLED = 
      "semimetric.remove.edges.enabled";

  /** Default value for removing semi-metric edges in the output graph. */
  public static final boolean REMOVE_EDGES_ENABLED_DEFAULT = true;


  /**
   * This class implements the first stage, which propagates the ID of a vertex
   * to all neighbors with higher ID.
   * 
   * We assume that 
   * 
   * @author dl
   *
   */
  public static class PropagateId extends AbstractComputation<LongWritable,
  NullWritable, DoubleWritable, Writable, LongWritable> {

    @Override
    public void compute(Vertex<LongWritable, NullWritable,DoubleWritable> vertex, 
        Iterable<Writable> messages) throws IOException {
      for (Edge<LongWritable, DoubleWritable> edge: vertex.getEdges()) {
        if (edge.getTargetVertexId().compareTo(vertex.getId()) > 0) {
          sendMessage(edge.getTargetVertexId(), vertex.getId());
        }
      }
      vertex.voteToHalt();
    }
  } 

  /**
   * This class implements the second phase of the algorithm that finds all
   * unique triangles (not just counting) them. The difference with the 
   * ForwardId implementation, is that it sends a pair of IDs: the ID included
   * in the message sent from the first phase, and the ID of the current vertex.
   * 
   * @author dl
   *
   */
  public static class ForwardEdge extends AbstractComputation<LongWritable, 
  Writable, DoubleWritable, LongWritable, SimpleEdge> {

    @Override
    public void compute(Vertex<LongWritable, Writable, DoubleWritable> vertex, 
        Iterable<LongWritable> messages) throws IOException {

      for (LongWritable msg : messages) {
        assert(msg.compareTo(vertex.getId())<0); // This can never happen

        double weight = vertex.getEdgeValue(msg).get();

        // This means there is an edge:
        // 1) FROM vertex with ID=msg.get()
        // 2) TO vertex with ID=vertex.getId().get()
        // 3) with the specified weight.
        SimpleEdge t = new SimpleEdge(msg.get(), vertex.getId().get(), weight);

        for (Edge<LongWritable, DoubleWritable> edge: vertex.getEdges()) {
          if (vertex.getId().compareTo(edge.getTargetVertexId()) < 0) {
            sendMessage(edge.getTargetVertexId(), t);
          }
        } 
      }
      vertex.voteToHalt();
    }
  }

  /**
   * This class implements the third phase of the algorithm that detects whether
   * a triangle has closed and whether there is a semi-metric edge in this
   * triangle.
   * 
   * @author dl
   *
   */
  @SuppressWarnings("rawtypes")
public static class FindSemimetricEdges extends AbstractComputation<LongWritable, 
    Writable, DoubleWritable, SimpleEdge, WritableComparable> {

    boolean removeEdgesEnabled;

    @Override
    public void preSuperstep() {
      removeEdgesEnabled = getContext().getConfiguration().getBoolean(
          REMOVE_EDGES_ENABLED, REMOVE_EDGES_ENABLED_DEFAULT);
    }

    @Override
    public void compute(Vertex<LongWritable, Writable, DoubleWritable> vertex, 
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
          
          double weight_ab = vertex.getEdgeValue(id2).get();
          double weight_ac = vertex.getEdgeValue(id1).get();
          double weight_bc = msg.getWeight(); 
          
          if (weight_ab+weight_ac < weight_bc) {
            if (removeEdgesEnabled) {
              removeEdgesRequest(id1, id2);
              removeEdgesRequest(id2, id1);
            }
          } else if (weight_ab+weight_bc < weight_ac) {
            if (removeEdgesEnabled) {
              removeEdgesRequest(vertex.getId(), id1);
              removeEdgesRequest(id1, vertex.getId());
            }
          } else if (weight_bc+weight_ac < weight_ab) {
            if (removeEdgesEnabled) {
              removeEdgesRequest(vertex.getId(), id2);
              removeEdgesRequest(id2, vertex.getId());
            }
          }
        }
      }
      
      // NOTE: In this phase, vertices do not halt. We need the next superstep
      // to run so that the edge removal requests execute.
    }
  }

  /**
   * We use this as the final phase of the algorithm. This does not perform any
   * computation. We run this computation only for the edge removal requests to
   * get executed.
   * 
   * @author dl
   *
   */
  public static class Finalize extends AbstractComputation<LongWritable, 
  NullWritable, DoubleWritable, Writable, Writable> {

    @Override
    public void compute(Vertex<LongWritable, NullWritable, DoubleWritable> vertex,
        Iterable<Writable> messages) throws IOException {
      vertex.voteToHalt();
    }
  }

  /**
   * Represents an undirected edge with a symmetric weight.
   * @author dl
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
   * Use this MasterCompute implementation to find the semi-metric edges.
   * 
   * @author dl
   *
   */
  public static class SemimetricMasterCompute extends DefaultMasterCompute {
    
    @Override
    public void compute() {
      boolean removeEdgesEnabled = getConf().getBoolean(
          REMOVE_EDGES_ENABLED, REMOVE_EDGES_ENABLED_DEFAULT);

      long superstep = getSuperstep();  
      if (superstep==0) {
        setComputation(PropagateId.class);
        setIncomingMessage(LongWritable.class);
        setOutgoingMessage(LongWritable.class);
      } else if (superstep==1) {
        setComputation(ForwardEdge.class);
        setIncomingMessage(LongWritable.class);
        setOutgoingMessage(SimpleEdge.class);
      } else if (superstep==2){
        setComputation(FindSemimetricEdges.class);
        setIncomingMessage(SimpleEdge.class);
        setOutgoingMessage(LongWritable.class);
      }  else {
        if (removeEdgesEnabled) {
          // We need this final superstep to run in order to remove the edges.
          setComputation(Finalize.class);
          setIncomingMessage(LongWritable.class);
          setOutgoingMessage(LongWritable.class);
        } else {
          // Otherwise we can stop here.
          haltComputation();
        }
      }
    }
  }
}