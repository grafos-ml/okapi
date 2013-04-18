/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package es.tid.graphlib.examples;

import org.apache.giraph.examples.Algorithm;
//import org.apache.giraph.examples.FloatWritable;
//import org.apache.giraph.examples.LongWritable;
import org.apache.giraph.graph.Vertex;
//import org.apache.giraph.vertex.LongDoubleFloatDoubleVertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Demonstrates the basic Pregel PageRank implementation.
 */
@Algorithm(
    name = "Page rank - modified"
)
public class SimplePageRankVertex extends Vertex<LongWritable,
  DoubleWritable, FloatWritable, DoubleWritable>  {
  /** Number of supersteps for this test */
  public static final int MAX_SUPERSTEPS = 30;
  /** Decimals */
  static int DECIMALS=4;
  /** Tolerance */
  static double TOLERANCE = 0.003;
  /** Initial value to be used for the L2Norm case */
  DoubleWritable initialValue = new DoubleWritable();
  /** L2Norm Error */
  public double l2normError = 0d;
  
  @Override
  public void compute(Iterable<DoubleWritable> messages) {
	  /** Flag for checking if parameter for L2Norm is enabled */
	  boolean l2normFlag = getContext().getConfiguration().getBoolean("PageRank.l2norm", false);
	  //System.out.println("PageRank: " + l2normFlag);	
		
	  if (getSuperstep() == 0){
		  if (l2normFlag) {
			  initialValue = getValue();
			  //System.out.println("**S: " + getSuperstep() + ", vertex, init_value: " + getId() + ", " + initialValue);
		  }
		  /** Set value with X decimals */
		  keepXdecimals(new DoubleWritable(1d/getTotalNumVertices()), DECIMALS);
	  }
	  if (getSuperstep() >= 1) {
		  double sum = 0;
		  for (DoubleWritable message : messages) {
			  sum += message.get();
		  }
		  DoubleWritable vertexValue =
				  new DoubleWritable((0.15f / getTotalNumVertices()) + 0.85f * sum);
		  setValue(vertexValue);
		  /** Compute L2Norm */
		  if (l2normFlag) {
			  l2normError = getL2Norm(initialValue, getValue());
			  System.out.println("**S: " + getSuperstep() + ", VertexId: " + getId() + ", [" + initialValue + ", " +getValue() + "], " + l2normError);
		  }
	  }

    	  if (getSuperstep() < MAX_SUPERSTEPS) {
      		long edges = getNumEdges();
      		if (getSuperstep() == 0 || l2normError > TOLERANCE){
    			sendMessageToAllEdges(
    			  	new DoubleWritable(getValue().get() / edges));
      		} else {
    	 		voteToHalt();
      		}
    	}
  }
  
	/*** Decimal Precision of latent vector values */
	public void keepXdecimals(DoubleWritable value, int x){
		double num=1;
		for (int i=0; i<x; i++){
			num*=10;
		}
		setValue(new DoubleWritable((double)(Math.round(getValue().get() * num) / num)));
	}
	/*** Calculate the L2Norm on the errors calculated by the current vertex */
	public double getL2Norm(DoubleWritable valOld, DoubleWritable valNew){
		return Math.sqrt(Math.pow(valOld.get() - valNew.get(),2));
	}
	
}
