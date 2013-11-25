package ml.grafos.okapi.cf.ranking;

import java.io.IOException;
import java.util.Random;

import ml.grafos.okapi.cf.CfLongId;
import ml.grafos.okapi.cf.FloatMatrixMessage;
import ml.grafos.okapi.cf.annotations.OkapiAutotuning;
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
 * Computes the random ranking 
 * @author linas
 *
 */
@OkapiAutotuning
public class RandomRankingComputation extends BasicComputation<CfLongId, FloatMatrixWritable, FloatWritable, FloatMatrixMessage> {

    static final FloatMatrixWritable emptyList = new FloatMatrixWritable(0);
    static final CfLongId nullId = new CfLongId();
    static final FloatMatrixMessage emptyMsg = new FloatMatrixMessage(nullId, emptyList, 0);

    static int DIM = 2;

    public static void setDim(int dim) {
        RandomRankingComputation.DIM = dim;
    }

    @Override
    public void compute(Vertex<CfLongId, FloatMatrixWritable, FloatWritable> vertex, Iterable<FloatMatrixMessage> messages) throws IOException {
        vertex.setValue(new FloatMatrixWritable(FloatMatrix.rand(DIM)));
        if(getSuperstep() == 0){
            Iterable<Edge<CfLongId, FloatWritable>> edges = vertex.getEdges();
            sendMessage(vertex.getId(), emptyMsg); //send message to myself in order to be executed in the next super step
            for (Edge<CfLongId, FloatWritable> edge : edges) {
                sendMessage(edge.getTargetVertexId(), new FloatMatrixMessage(vertex.getId(), emptyList, edge.getValue().get()));
            }
		}
		vertex.voteToHalt();
	}

    public static int getDim() {
        return DIM;
    }
}
