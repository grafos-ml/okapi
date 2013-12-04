package ml.grafos.okapi.cf.eval;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Iterator;

import ml.grafos.okapi.cf.CfLongId;
import ml.grafos.okapi.common.jblas.FloatMatrixWritable;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.jblas.FloatMatrix;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/*
 * 0 -1
 * 32729 0	[0.883140; 0.126675]    5007,1384,304
 * 7563 0	[0.544951; 0.719476]    1384,304
 * 5007 1	[0.726413; 0.968422]
 * 1384 1	[0.933587; 0.755566]
 */


public class CfModelTestingInputFormatTest extends CfModelTestingInputFormat{

	ImmutableClassesGiraphConfiguration<CfLongId, FloatMatrixWritable, BooleanWritable> conf;
	RecordReader<LongWritable, Text> rr;
	CfModelTestingInputFormatTest labi;
	
	public CfModelTestingInputFormatTest() throws IOException, InterruptedException{
		super();
		this.init();
	}
	
	 @Before
	 public void setUp() throws IOException, InterruptedException {
		 labi = new CfModelTestingInputFormatTest();
	 }
	
	void init() throws IOException, InterruptedException{
		rr = mock(RecordReader.class);
		when(rr.nextKeyValue()).thenReturn(true).thenReturn(false);
		conf = new ImmutableClassesGiraphConfiguration(new GiraphConfiguration());
	}
	
	@Override
	public TextVertexReader createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new LongArrayBooleanVertexReader() {
			@Override
		      protected RecordReader<LongWritable, Text> getRecordReader(){
		        return rr;
		      }
		    };
	}
	
	@Test
	public void testVertexReader() throws IOException, InterruptedException {
		when(labi.rr.getCurrentValue()).thenReturn(new Text("32729 0	[0.883140; 0.126675]	5007,1384,304"));
		VertexReader<CfLongId, FloatMatrixWritable, BooleanWritable> vertexReader = labi.createVertexReader(null, null);
		vertexReader.setConf(conf);
		Vertex<CfLongId, FloatMatrixWritable, BooleanWritable> currentVertex = vertexReader.getCurrentVertex();
		
		Assert.assertEquals(new CfLongId((byte)0, 32729), currentVertex.getId());
		FloatMatrixWritable matrix = new FloatMatrixWritable(new FloatMatrix(new float[]{0.883140f, 0.126675f}));
		Assert.assertEquals(matrix, currentVertex.getValue());
		Assert.assertEquals(3, currentVertex.getNumEdges());
	}
	
	@Test
	public void testItemVertexReader() throws IOException, InterruptedException {
		when(labi.rr.getCurrentValue()).thenReturn(new Text("32729 1	[0.883140; 0.126675]"));
		VertexReader<CfLongId, FloatMatrixWritable, BooleanWritable> vertexReader = labi.createVertexReader(null, null);
		vertexReader.setConf(conf);
		Vertex<CfLongId, FloatMatrixWritable, BooleanWritable> currentVertex = vertexReader.getCurrentVertex();
		
		Assert.assertEquals(new CfLongId((byte)1, 32729), currentVertex.getId());
		FloatMatrixWritable matrix = new FloatMatrixWritable(new FloatMatrix(new float[]{0.883140f, 0.126675f}));
		Assert.assertEquals(matrix, currentVertex.getValue());
		Assert.assertEquals(0, currentVertex.getNumEdges());
	}
	
	@Test
	public void testNullVertex() throws IOException, InterruptedException{
		when(labi.rr.getCurrentValue()).thenReturn(new Text("0 -1"));
		VertexReader<CfLongId, FloatMatrixWritable, BooleanWritable> vertexReader = labi.createVertexReader(null, null);
		vertexReader.setConf(conf);
		Vertex<CfLongId, FloatMatrixWritable, BooleanWritable> currentVertex = vertexReader.getCurrentVertex();
		
		Assert.assertEquals(new CfLongId((byte)-1, 0), currentVertex.getId());
		FloatMatrixWritable matrix = new FloatMatrixWritable(0);
		Assert.assertEquals(matrix, currentVertex.getValue());
		Assert.assertEquals(0, currentVertex.getNumEdges());
	}

}
