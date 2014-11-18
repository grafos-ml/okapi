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
package ml.grafos.okapi.graphs.similarity;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import ml.grafos.okapi.common.computation.SendFriends;
import ml.grafos.okapi.common.data.LongArrayListWritable;
import ml.grafos.okapi.common.data.MessageWrapper;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

import com.google.common.primitives.Longs;

/**
 * 
 * This class computes the Jaccard similarity or distance
 * for each pair of neighbors in an undirected unweighted graph.  
 * 
 * To get the exact Jaccard similarity, run the command:
 * 
 * <pre>
 * hadoop jar $OKAPI_JAR org.apache.giraph.GiraphRunner \
 *   ml.grafos.okapi.graphs.Jaccard\$SendFriendsList  \
 *   -mc  ml.grafos.okapi.graphs.Jaccard\$MasterCompute  \
 *   -eif ml.grafos.okapi.io.formats.LongDoubleZerosTextEdgeInputFormat  \
 *   -eip $INPUT_EDGES \
 *   -eof org.apache.giraph.io.formats.SrcIdDstIdEdgeValueTextOutputFormat \
 *   -op $OUTPUT \
 *   -w $WORKERS \
 *   -ca giraph.oneToAllMsgSending=true \
 *   -ca giraph.outEdgesClass=org.apache.giraph.edge.HashMapEdges \
 *   -ca jaccard.approximation.enabled=false
 *   
 *   Use -ca distance.conversion.enabled=true to get the Jaccard distance instead.
 *
 * 
 * To get the approximate Jaccard similarity, replace the SendFriendsList class
 * in the command with the SendFriendsBloomFilter class and set the 
 * jaccard.approximation.enabled parameter to true.
 * 
 * 
 * @author dl
 *
 */
public class Jaccard {

  /** Enables the approximation computation */
  public static final String JACCARD_APPROXIMATION = 
      "jaccard.approximation.enabled";
  
  /** Default value for approximate computation */
  public static final boolean JACCARD_APPROXIMATION_DEFAULT = false;

  /** Size of bloom filter in bits */
  public static final String BLOOM_FILTER_BITS = "jaccard.bloom.filter.bits";
  
  /** Default size of bloom filters */
  public static final int BLOOM_FILTER_BITS_DEFAULT = 16;
  
  /** Number of functions to use in bloom filter */
  public static final String BLOOM_FILTER_FUNCTIONS = "jaccard.bloom.filter.functions";
  
  /** Default number of functions to use in bloom filter */
  public static final int BLOOM_FILTER_FUNCTIONS_DEFAULT = 1;
  
  /** Type of hash function to use in bloom filter */
  public static final String BLOOM_FILTER_HASH_TYPE = "jaccard.bloom.filter.hash.type";

  /** Default type of hash function in bloom filter */
  public static final int BLOOM_FILTER_HASH_TYPE_DEFAULT = Hash.MURMUR_HASH;
  
  /** Enables the conversion to distance conversion */
  public static final String DISTANCE_CONVERSION = 
      "distance.conversion.enabled";
  
  /** Default value for distance conversion */
  public static final boolean DISTANCE_CONVERSION_DEFAULT = false;

  /**
   * Implements the first step in the exact jaccard similirity algorithm. Each
   * vertex broadcasts the list with the IDs of al its neighbors.
   * @author dl
   *
   */
  public static class SendFriendsList extends SendFriends<LongWritable, 
    NullWritable, DoubleWritable, LongIdFriendsList> {
  }

  /**
   * This is the message sent in the implementation of the exact jaccard
   * similarity. The message contains the source vertex id and a list of vertex
   * ids representing the neighbors of the source.
   * 
   * @author dl
   *
   */
  public static class LongIdFriendsList extends MessageWrapper<LongWritable, 
    LongArrayListWritable> { 

    @Override
    public Class<LongWritable> getVertexIdClass() {
      return LongWritable.class;
    }

    @Override
    public Class<LongArrayListWritable> getMessageClass() {
      return LongArrayListWritable.class;
    }
  }

  /**
   * Implements the computation of the exact Jaccard vertex similarity. The 
   * vertex Jaccard similarity between u and v is the number of common neighbors 
   * of u and v divided by the number of vertices that are neighbors of u or v.
   * 
   * This computes similarity only between vertices that are connected with 
   * edges, not any pair of vertices in the graph.
   * 
   * @author dl
   *
   */
  public static class JaccardComputation extends BasicComputation<LongWritable, 
    NullWritable, DoubleWritable, LongIdFriendsList> {
	  
	  boolean conversionEnabled;
	  
	  @Override
	  public void preSuperstep() {
		  conversionEnabled = getConf().getBoolean(DISTANCE_CONVERSION, DISTANCE_CONVERSION_DEFAULT);
	  }

    @Override
    public void compute(
        Vertex<LongWritable, NullWritable, DoubleWritable> vertex,
        Iterable<LongIdFriendsList> messages) throws IOException {

      for (LongIdFriendsList msg : messages) {
        LongWritable src = msg.getSourceId();
        DoubleWritable edgeValue = vertex.getEdgeValue(src);
        assert(edgeValue!=null);
        long totalFriends = vertex.getNumEdges();
        long commonFriends = 0;
        for (LongWritable id : msg.getMessage()) {
          if (vertex.getEdgeValue(id)!=null) { // This is a common friend
            commonFriends++;
          } else {
            totalFriends++;
          }
        }
        // The Jaccard similarity is commonFriends/totalFriends
        // If the edge to the vertex with ID src does not exist, which is the
        // case in a directed graph, this call has no effect. 
        vertex.setEdgeValue(src, new DoubleWritable(
            (double)commonFriends/(double)totalFriends));
      }
      if (!conversionEnabled) {
    	  vertex.voteToHalt();
      }
    }
  }
  
  public static class ScaleToDistance extends BasicComputation<LongWritable, 
  	NullWritable, DoubleWritable, LongIdFriendsList> {

	@Override
	public void compute(
			Vertex<LongWritable, NullWritable, DoubleWritable> vertex,
			Iterable<LongIdFriendsList> messages) throws IOException {
		
		for (Edge<LongWritable, DoubleWritable> e: vertex.getEdges()) {
			vertex.setEdgeValue(e.getTargetVertexId(), covertToDistance(e.getValue()));
		}
		vertex.voteToHalt();
	}	  
  }



  /**
   * This class implements the first computation step in the approximate
   * jaccard similarity algorithm. A vertex creates a bloom filter and adds the
   * IDs of its neighbors, and broadcasts it to all its neighbors along with
   * its own ID and the number of neighbors it has.
   * @author dl
   *
   */
  public static class SendFriendsBloomFilter extends BasicComputation<LongWritable, 
    NullWritable, DoubleWritable, LongIdBloomFilter> {

    int numBits;
    int numFunctions;
    int hashType;

    @Override
    public void preSuperstep() {
      numBits = getConf().getInt(
          BLOOM_FILTER_BITS, BLOOM_FILTER_BITS_DEFAULT);
      numFunctions = getConf().getInt(
          BLOOM_FILTER_FUNCTIONS, BLOOM_FILTER_FUNCTIONS_DEFAULT);
      hashType = getConf().getInt(
          BLOOM_FILTER_HASH_TYPE, BLOOM_FILTER_HASH_TYPE_DEFAULT);
    }

    @Override
    public void compute(
        Vertex<LongWritable, NullWritable, DoubleWritable> vertex,
        Iterable<LongIdBloomFilter> messages) throws IOException {
      
      BloomFilter filter = new BloomFilter(numBits, numFunctions, hashType);

      for (Edge<LongWritable, DoubleWritable> e : vertex.getEdges()) {
        filter.add(new Key(Longs.toByteArray(e.getTargetVertexId().get())));
      }

      sendMessageToAllEdges(vertex, 
          new LongIdBloomFilter(vertex.getId(), filter, vertex.getNumEdges()));
    }
  }

  /**
   * This is the message sent in the approximate jaccard similiarity 
   * implementation. In this implementation, the message carries (i) the source
   * id of the message, (ii) the bloom filter containing vertex IDs and
   * (iii) an integer indicating the number of elements in the bloom filter.
   * 
   * @author dl
   *
   */
  public static class LongIdBloomFilter extends MessageWrapper<LongWritable, 
    BloomFilter> {

    private int numElements;

    public LongIdBloomFilter() {
      super();
    }

    public LongIdBloomFilter(LongWritable src, BloomFilter msg, 
        int numElements) {
      super(src, msg);
      this.numElements = numElements;
    }

    @Override
    public Class<LongWritable> getVertexIdClass() {
      return LongWritable.class;
    }

    @Override
    public Class<BloomFilter> getMessageClass() {
      return BloomFilter.class;
    }
    
    public int getNumElements() {
      return numElements;
    }
    
    @Override
    public void write(DataOutput output) throws IOException {
      super.write(output);
      output.writeInt(numElements);
    }
    
    @Override
    public void readFields(DataInput input) throws IOException {
      super.readFields(input);
      numElements = input.readInt();
    }
  }

  /**
   * Implements an approximation of the Jaccard vertex similarity. In this
   * implementation, a vertex does not broadcast its entire neighbor list, but
   * a compact summarization of it in the form of a bloom filter. When this
   * summarization is received, the destination vertices test for common
   * neighbors against the bloom filter. Due to the possibility of false
   * positives, vertices may overestimate the number of common neighbors.
   * 
   * @author dl
   *
   */
  public static class JaccardApproximation extends BasicComputation<LongWritable, 
    NullWritable, DoubleWritable, LongIdBloomFilter> {

	  boolean conversionEnabled;
	  
	  @Override
	  public void preSuperstep() {
		  conversionEnabled = getConf().getBoolean(DISTANCE_CONVERSION, DISTANCE_CONVERSION_DEFAULT);
	  }
	  
    @Override
    public void compute(
        Vertex<LongWritable, NullWritable, DoubleWritable> vertex,
        Iterable<LongIdBloomFilter> messages) throws IOException {

      for (LongIdBloomFilter msg : messages) {
        LongWritable src = msg.getSourceId();
        BloomFilter filter = msg.getMessage();
        DoubleWritable edgeValue = vertex.getEdgeValue(src);
        assert(edgeValue!=null);
        long totalFriends = msg.getNumElements();
        long commonFriends = 0;
        for (Edge<LongWritable, DoubleWritable> e : vertex.getEdges()) {
          Key k = new Key(Longs.toByteArray(e.getTargetVertexId().get()));
          if (filter.membershipTest(k)) { // This is a common friend
            commonFriends++;
          } else {
            totalFriends++;
          }
        }
        // The Jaccard similarity is commonFriends/totalFriends
        // If the edge to the vertex with ID src does not exist, which is the
        // case in a directed graph, this call has no effect. 
        vertex.setEdgeValue(src, new DoubleWritable(
        		 (double)Math.min(commonFriends, totalFriends)/(double)totalFriends));
      }
      if (!conversionEnabled) {
    	  vertex.voteToHalt();
      }
    }
  }
  
  public static class ScaleToDistanceBloom extends BasicComputation<LongWritable, 
	NullWritable, DoubleWritable, LongIdBloomFilter> {

	@Override
	public void compute(
			Vertex<LongWritable, NullWritable, DoubleWritable> vertex,
			Iterable<LongIdBloomFilter> messages) throws IOException {
		
		for (Edge<LongWritable, DoubleWritable> e: vertex.getEdges()) {
			vertex.setEdgeValue(e.getTargetVertexId(), covertToDistance(e.getValue()));
		}
		vertex.voteToHalt();
	}	  
}
  
  /**
	 * 
	 * Converts the [0,1] similarity value to a distance
	 * which takes values in [0, INF].
	 * 
	 */
	private static DoubleWritable covertToDistance(DoubleWritable value) {
		if (Math.abs(value.get()) > 0) {
			value.set((1.0 / value.get()) - 1.0);
		}
		else {
			value.set(Double.MAX_VALUE);
		}
		return value;
	}


  /**
   * Coordinates the execution of the algorithm.
   */
  public static class MasterCompute extends DefaultMasterCompute {

    boolean approximationEnabled;
    boolean conversionEnabled;

    @Override
    public final void initialize() throws InstantiationException,
        IllegalAccessException {
      approximationEnabled = getConf().getBoolean(
          JACCARD_APPROXIMATION, JACCARD_APPROXIMATION_DEFAULT);
      conversionEnabled = getConf().getBoolean(DISTANCE_CONVERSION, DISTANCE_CONVERSION_DEFAULT);
    }

    @Override
    public final void compute() {
      long superstep = getSuperstep();
      if (approximationEnabled) {
        if (superstep == 0) {
          setComputation(SendFriendsBloomFilter.class);
        } else if (superstep == 1) {
          setComputation(JaccardApproximation.class);
        } else {
        	if (conversionEnabled) {
        		setComputation(ScaleToDistanceBloom.class);
        	}
        }
      } else {
        if (superstep == 0) {
          setComputation(SendFriendsList.class);
        } else if (superstep == 1) {
          setComputation(JaccardComputation.class);
        } else {
        	if (conversionEnabled) {
        		setComputation(ScaleToDistance.class);
        	}
        }
        
      }
    }
  }
}