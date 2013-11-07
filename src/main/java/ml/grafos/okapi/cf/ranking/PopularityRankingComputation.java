package ml.grafos.okapi.cf.ranking;

import java.io.IOException;

import ml.grafos.okapi.cf.eval.DoubleArrayListWritable;
import ml.grafos.okapi.cf.eval.LongDoubleArrayListMessage;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;


/**
 * Computes the popularity ranking based on the number of of times a user rated/seen the item.
 * The idea is to represent users and items as bipartite graph. Then compute how many messages
 * each item received which is equal to the number of users rated the item.
 * @author linas
 *
 */
public class PopularityRankingComputation extends BasicComputation<LongWritable, DoubleArrayListWritable, IntWritable, LongDoubleArrayListMessage>{

	private static final DoubleArrayListWritable emptyList = new DoubleArrayListWritable();
	private static final LongDoubleArrayListMessage emptyMsg = new LongDoubleArrayListMessage(0, emptyList, 0);
	
	@Override
	public void compute(
			Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex,
			Iterable<LongDoubleArrayListMessage> messages) throws IOException {
		if (getSuperstep() == 0){//send empty message with the count
			if (vertex.getId().get() > 0){
				Iterable<Edge<LongWritable, IntWritable>> edges = vertex.getEdges();
				sendMessage(vertex.getId(), emptyMsg); //send message to myself in order to be exectured in the next superstep
				for (Edge<LongWritable, IntWritable> edge : edges) {
					sendMessage(edge.getTargetVertexId(), new LongDoubleArrayListMessage(vertex.getId().get(), emptyList, edge.getValue().get()));
				}
			}
		}else if(getSuperstep() == 1){//compute how many messages were sent
			int cnt = 0;
			DoubleArrayListWritable output = new DoubleArrayListWritable();
			if (vertex.getId().get() < 0){
				for (LongDoubleArrayListMessage msg : messages) {
					cnt+= msg.getScore();
				}
				output.add(new DoubleWritable(cnt));
			}else if (vertex.getId().get() > 0){
				output.add(new DoubleWritable(1.0));
			}
			output.add(new DoubleWritable(0.0));
			vertex.setValue(output);
		}
		vertex.voteToHalt();
	}
}
