package ml.grafos.okapi.cf.eval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Random;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Computes a rank measure using giraph infrastructure;
 * As a message we use DoubleArrayListWritable.
 * @author linas
 *
 */
public class RankEvaluationComputation extends BasicComputation<LongWritable, DoubleArrayListWritable, BooleanWritable, LongDoubleArrayListMessage>{

	private static int numberSamples;
	private static long minItemId;
	private static long maxItemId;
	private static final DoubleArrayListWritable emptyList = new DoubleArrayListWritable();
	private static int k;
	
	@Override
	public void compute(
			Vertex<LongWritable, DoubleArrayListWritable, BooleanWritable> vertex,
			Iterable<LongDoubleArrayListMessage> messages) throws IOException {
		minItemId = Integer.parseInt(getConf().get("minItemId"));
		maxItemId = Integer.parseInt(getConf().get("maxItemId"));
		numberSamples = Integer.parseInt(getConf().get("numberSamples"));
		k = Integer.parseInt(getConf().get("k", "5"));
		if (getSuperstep() == 0){
			sampleIrrelevantEdges(vertex);
			sendUserDataToItems(vertex);
		}else if(getSuperstep() == 1){
			computeScoreAndSendBack(vertex, messages);
		}else if(getSuperstep() == 2){
			computeRankingMeasure(vertex, messages);
		}else if (getSuperstep() == 3){
			computeTheFinalScore(vertex, messages);
		}
		vertex.voteToHalt();
	}

	public void computeTheFinalScore(
			Vertex<LongWritable, DoubleArrayListWritable, BooleanWritable> vertex,
			Iterable<LongDoubleArrayListMessage> messages) {
		if (vertex.getId().get() == 0){//special vertex which computes the final score
			double sum = 0;
			int cnt = 0;
			for (LongDoubleArrayListMessage msg : messages) {
				sum += msg.getScore();
				cnt += 1;
			}
			double avg = sum/cnt;
			DoubleArrayListWritable finalScore = new DoubleArrayListWritable();
			finalScore.add(new DoubleWritable(avg));
			vertex.setValue(finalScore);
		}
	}

	public void computeRankingMeasure(
			Vertex<LongWritable, DoubleArrayListWritable, BooleanWritable> vertex,
			Iterable<LongDoubleArrayListMessage> messages) {
		if (vertex.getId().get() > 0){//users are positives, items are negatives, 0 can not extist.
			ArrayList<DoubleBoolean> scores = new ArrayList<DoubleBoolean>(); 
			for(LongDoubleArrayListMessage msg : messages){
				scores.add(new DoubleBoolean(msg.getScore(), vertex.getEdgeValue(new LongWritable(msg.getSenderId())).get()));
			}
			Collections.sort(scores);
			double rankingMeasure = computeRecall(scores, this.k);
			sendMessage(new LongWritable(0), new LongDoubleArrayListMessage(vertex.getId().get(), emptyList, rankingMeasure));
		}
	}

	private double computeRecall(ArrayList<DoubleBoolean> scores, int k) {
		if (null == scores || scores.size() == 0)
			return 0;
		int cnt = 0;
		double relevant = 0;
		for (DoubleBoolean doubleBoolean : scores) {
			if (doubleBoolean.isRelevant())
				relevant += 1;
			cnt += 1;
			if (cnt >= k)
				break;
		}
		return relevant/cnt;
	}

	public void computeScoreAndSendBack(
			Vertex<LongWritable, DoubleArrayListWritable, BooleanWritable> vertex,
			Iterable<LongDoubleArrayListMessage> messages) {
		if (vertex.getId().get() < 0){//only items send messages
			for(LongDoubleArrayListMessage msg : messages){
				double score = dotProduct(msg.getFactors(), vertex.getValue());
				LongDoubleArrayListMessage msgToSendBack = new LongDoubleArrayListMessage(vertex.getId().get(), emptyList, score);
				sendMessage(new LongWritable(msg.getSenderId()), msgToSendBack);
			}
		}
	}

	private double dotProduct(DoubleArrayListWritable factors,
			DoubleArrayListWritable value) {
		int size = factors.size();
		if (size != value.size())
			return 0.0;
		double sum = 0;
		for(int i=0; i<size; i++){
			sum += factors.get(i).get()*value.get(i).get();
		}
		return sum;
	}

	public void sendUserDataToItems(
			Vertex<LongWritable, DoubleArrayListWritable, BooleanWritable> vertex) {
		if (vertex.getId().get() > 0){//users are positives, items are negatives, 0 can not extist.
			LongDoubleArrayListMessage msg = new LongDoubleArrayListMessage(vertex.getId().get(), vertex.getValue(), -1.0);
			sendMessageToAllEdges(vertex, msg);
		}
	}

	public void sampleIrrelevantEdges(
			Vertex<LongWritable, DoubleArrayListWritable, BooleanWritable> vertex) {
		if (vertex.getId().get() > 0){//only users
			Iterable<Edge<LongWritable, BooleanWritable>> edges = vertex.getEdges();
			HashSet<Long> relevant = new HashSet<Long>();
			for (Edge<LongWritable, BooleanWritable> e : edges) {
				relevant.add(e.getTargetVertexId().get());
			}
			for (int i=0; i<numberSamples; i++){
				long randomItemId = getRandomItemId(relevant);
				vertex.addEdge(EdgeFactory.create(new LongWritable(randomItemId), new BooleanWritable(false)));
			}
		}
	}

	private long getRandomItemId(HashSet<Long> relevant) {
		Random r = new Random();
		long i = r.nextInt((int)(maxItemId-minItemId))+minItemId;
		int maxCnt = 0;
		while (relevant.contains(i)){
			i = r.nextInt((int)(maxItemId-minItemId))+minItemId;
			if (maxCnt > 1000000){
				throw new RuntimeException("Can not sample a new irrelevant item");
			}
			maxCnt += 1;
		}
		return i;
	}

}
