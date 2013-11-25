package ml.grafos.okapi.cf.ranking;

import java.io.IOException;
import java.util.Random;

import ml.grafos.okapi.cf.CfLongId;
import ml.grafos.okapi.cf.FloatMatrixMessage;
import ml.grafos.okapi.cf.annotations.OkapiAutotuning;
import ml.grafos.okapi.cf.eval.DoubleArrayListWritable;
import ml.grafos.okapi.cf.eval.LongDoubleArrayListMessage;

import ml.grafos.okapi.common.jblas.FloatMatrixWritable;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.jblas.FloatMatrix;


/**
 * Computes the random ranking 
 * @author linas
 *
 */
@OkapiAutotuning
public class RandomRankingComputation extends BasicComputation<CfLongId, FloatMatrixWritable, FloatWritable, FloatMatrixMessage> {

	@Override
    public void compute(Vertex<CfLongId, FloatMatrixWritable, FloatWritable> vertex, Iterable<FloatMatrixMessage> messages) throws IOException {
        if(getSuperstep() == 0){
            vertex.setValue(new FloatMatrixWritable(FloatMatrix.rand(5)));
		}
		vertex.voteToHalt();
	}
}
