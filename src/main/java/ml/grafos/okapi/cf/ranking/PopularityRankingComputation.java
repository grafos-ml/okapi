package ml.grafos.okapi.cf.ranking;

import java.io.IOException;

import ml.grafos.okapi.cf.CfLongId;
import ml.grafos.okapi.cf.FloatMatrixMessage;
import ml.grafos.okapi.cf.eval.DoubleArrayListWritable;
import ml.grafos.okapi.cf.eval.LongDoubleArrayListMessage;

import ml.grafos.okapi.common.jblas.FloatMatrixWritable;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.jblas.FloatMatrix;


/**
 * Computes the popularity ranking based on the number of of times a user rated/seen the item.
 * The idea is to represent users and items as bipartite graph. Then compute how many messages
 * each item received which is equal to the number of users rated the item.
 * @author linas
 *
 */
public class PopularityRankingComputation extends BasicComputation<CfLongId, FloatMatrixWritable, FloatWritable, FloatMatrixMessage> {

    static final FloatMatrixWritable emptyList = new FloatMatrixWritable();
    static final FloatMatrixMessage emptyMsg = new FloatMatrixMessage(null, emptyList, 0);
	
	@Override
    public void compute(Vertex<CfLongId, FloatMatrixWritable, FloatWritable> vertex, Iterable<FloatMatrixMessage> messages) throws IOException {
        if (getSuperstep() == 0){//send empty message with the count
			if (vertex.getId().isUser()){
				Iterable<Edge<CfLongId, FloatWritable>> edges = vertex.getEdges();
				sendMessage(vertex.getId(), emptyMsg); //send message to myself in order to be exectured in the next superstep
				for (Edge<CfLongId, FloatWritable> edge : edges) {
					sendMessage(edge.getTargetVertexId(), new FloatMatrixMessage(vertex.getId(), emptyList, edge.getValue().get()));
				}
			}
		}else if(getSuperstep() == 1){//compute how many messages were sent
			int cnt = 0;
            FloatMatrix output = FloatMatrix.ones(1);
            float score = 0f;
			if (vertex.getId().isItem()){
				for (FloatMatrixMessage msg : messages) {
					cnt+= msg.getScore();
				}
				output.put(0, cnt);
			}
			vertex.setValue(new FloatMatrixWritable(output));
		}
		vertex.voteToHalt();
	}
}
