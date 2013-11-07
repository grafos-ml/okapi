package ml.grafos.okapi.cf.eval;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import static org.junit.Assert.*;

import ml.grafos.okapi.cf.eval.DoubleArrayListWritable;
import ml.grafos.okapi.cf.eval.LongArrayBooleanInputFormat;

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.formats.IntNullTextEdgeInputFormat;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.Before;
import org.junit.Test;


public class LongArrayBooleanInputFormatTest extends LongArrayBooleanInputFormat{

	LongArrayBooleanInputFormatTest labi;
	RecordReader<LongWritable, Text> rr;
	

	private ImmutableClassesGiraphConfiguration<LongWritable, DoubleArrayListWritable, BooleanWritable> conf;
	
	@Before
	public void setUp() throws Exception {
		labi = new LongArrayBooleanInputFormatTest();
	}
	
	void init() throws IOException, InterruptedException{
		rr = mock(RecordReader.class);
		when(rr.nextKeyValue()).thenReturn(true).thenReturn(false);
		conf = new ImmutableClassesGiraphConfiguration(new GiraphConfiguration());
	}

	@Override
	public org.apache.giraph.io.formats.TextVertexInputFormat.TextVertexReader createVertexReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return createVertexReader();
	}
	
	private org.apache.giraph.io.formats.TextVertexInputFormat.TextVertexReader createVertexReader() {
		return new LongArrayBooleanVertexReader() {
			@Override
		      protected RecordReader<LongWritable, Text> getRecordReader(){
		        return rr;
		      }
		    };
	}
	
	@Override
	public ImmutableClassesGiraphConfiguration<LongWritable, DoubleArrayListWritable, BooleanWritable> getConf() {
		return conf;
	}
	
	public LongArrayBooleanInputFormatTest() throws Exception {
		super();
		this.init();
	}

	@Test
	public void testVertexReader() throws IOException, InterruptedException {
		when(labi.rr.getCurrentValue()).thenReturn(new Text("1	0.01,0.02,-0.1	-1,-2,-3"));
		VertexReader<LongWritable, DoubleArrayListWritable, BooleanWritable> vertexReader = labi.createVertexReader(null, null);
		vertexReader.setConf(conf);
		Vertex<LongWritable, DoubleArrayListWritable, BooleanWritable> currentVertex = vertexReader.getCurrentVertex();
		assertEquals(new LongWritable(1), currentVertex.getId());
		DoubleArrayListWritable daw = new DoubleArrayListWritable();
		daw.add(new DoubleWritable(0.01));
		daw.add(new DoubleWritable(0.02));
		daw.add(new DoubleWritable(-0.1));
		assertEquals(daw, currentVertex.getValue());
		assertEquals(3, currentVertex.getNumEdges());

		/*
		//test the values of the reference nodes. we skip because it was too difficult task :)
		Iterable<Edge<LongWritable, BooleanWritable>> edges = currentVertex.getEdges();
		Iterator<Edge<LongWritable, BooleanWritable>> iterator = edges.iterator();
		
		Edge<LongWritable, BooleanWritable> next = iterator.next();
		assertEquals(new LongWritable(100), next.getTargetVertexId());
		assertEquals(new BooleanWritable(true), next.getValue());
		next = iterator.next();
		assertEquals(new LongWritable(101), next.getTargetVertexId());
		assertEquals(new BooleanWritable(true), next.getValue());
		next = iterator.next();
		assertEquals(new LongWritable(102), next.getTargetVertexId());
		assertEquals(new BooleanWritable(true), next.getValue());
		*/
	}
	
	@Test
	public void testItemVertexReader() throws IOException, InterruptedException {
		when(labi.rr.getCurrentValue()).thenReturn(new Text("100	0.01,0.02,-0.1	"));
		VertexReader<LongWritable, DoubleArrayListWritable, BooleanWritable> vertexReader = labi.createVertexReader(null, null);
		vertexReader.setConf(conf);
		Vertex<LongWritable, DoubleArrayListWritable, BooleanWritable> currentVertex = vertexReader.getCurrentVertex();
		assertEquals(new LongWritable(100), currentVertex.getId());
		DoubleArrayListWritable daw = new DoubleArrayListWritable();
		daw.add(new DoubleWritable(0.01));
		daw.add(new DoubleWritable(0.02));
		daw.add(new DoubleWritable(-0.1));
		assertEquals(daw, currentVertex.getValue());
		assertEquals(0, currentVertex.getNumEdges());
	}
}
