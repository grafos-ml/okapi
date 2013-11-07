package ml.grafos.okapi.cf.ranking;

import java.io.IOException;
import java.util.Random;

import ml.grafos.okapi.cf.annotations.OkapiAutotuning;
import ml.grafos.okapi.cf.eval.DoubleArrayListWritable;
import ml.grafos.okapi.cf.eval.LongDoubleArrayListMessage;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;


/**
 * Computes the random ranking 
 * @author linas
 *
 */
@OkapiAutotuning
public class RandomRankingComputation extends BasicComputation<LongWritable, DoubleArrayListWritable, IntWritable, LongDoubleArrayListMessage>{

	private final static Random random = new Random();
	@Override
	public void compute(
			Vertex<LongWritable, DoubleArrayListWritable, IntWritable> vertex,
			Iterable<LongDoubleArrayListMessage> messages) throws IOException {
		if(getSuperstep() == 0){
			DoubleArrayListWritable output = new DoubleArrayListWritable();
			if (vertex.getId().get() < 0){
				output.add(new DoubleWritable(random.nextInt(10000)));
			}else if (vertex.getId().get() > 0){
				output.add(new DoubleWritable(1.0));
			}
			output.add(new DoubleWritable(0.0));
			vertex.setValue(output);
		}
		vertex.voteToHalt();
	}
}
