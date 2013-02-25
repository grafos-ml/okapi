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
package es.tid.graphlib.sgd;

import org.apache.giraph.graph.DefaultEdge;
import org.apache.giraph.graph.Edge;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;
import org.apache.mahout.math.function.IntIntProcedure;
import org.apache.mahout.math.list.IntArrayList;
import org.apache.mahout.math.map.OpenIntIntHashMap;
import com.google.common.collect.UnmodifiableIterator;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

/**
 * Optimized vertex implementation for
 * <IntWritable, FloatWritable, IntWritable, IntWritable>
 * 
 * @param <I> Vertex index value
 * @param <V> Vertex value
 * @param <E> Edge value - neighbor vertex index value & rating
 * @param <M> Message value - Vertex coordinates
 */
public abstract class IntArrayIntIntVertex
    extends Vertex<IntWritable, FloatWritable, IntWritable, IntWritable>{		
	
  /** Class logger */
  private static final Logger LOG =
      Logger.getLogger(IntArrayIntIntVertex.class);
  /** Stores the edges */
  private OpenIntIntHashMap edgeMap =
      new OpenIntIntHashMap();

  @Override
  public void setEdges(Iterable<Edge<IntWritable, IntWritable>> edges) {
    if (edges != null) {
      for (Edge<IntWritable, IntWritable> edge : edges) {
        edgeMap.put(edge.getTargetVertexId().get(), edge.getValue().get());
      }
    }
  }

  @Override
  public Iterable<Edge<IntWritable, IntWritable>> getEdges() {
    final int[] targetVertices = edgeMap.keys().elements();
    final int numEdges = edgeMap.size();

    return new Iterable<Edge<IntWritable, IntWritable>>() {
      @Override
      public Iterator<Edge<IntWritable, IntWritable>> iterator() {
        return new Iterator<Edge<IntWritable, IntWritable>>() {
          private int offset = 0;

          @Override
          public boolean hasNext() {
            return offset < numEdges;
          }

          @Override
          public Edge<IntWritable, IntWritable> next() {
            int targetVertex = targetVertices[offset++];
            return new DefaultEdge<IntWritable, IntWritable>(
                new IntWritable(targetVertex),
                new IntWritable(targetVertex));
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException(
                "Mutation disallowed for edge list via iterator");
          }
        };
      }
    };
  }

  @Override
  public boolean hasEdge(IntWritable targetVertexId) {
    return edgeMap.containsKey(targetVertexId.get());
  }

  @Override
  public int getNumEdges() {
    return edgeMap.size();
  }

  @Override
  public final void readFields(DataInput in) throws IOException {
    int id = in.readInt();
    float value = in.readFloat();
    initialize(new IntWritable(id), new FloatWritable(value));
    edgeMap.clear();
    long edgeMapSize = in.readInt();
    for (long i = 0; i < edgeMapSize; ++i) {
      int targetVertexId = in.readInt();
      int edgeValue = in.readInt();
      edgeMap.put(targetVertexId, edgeValue);
    }
    readHaltBoolean(in);
  }

  //@Override
  public final void write(final DataOutput out) throws IOException {
    out.writeInt(getId().get());
    out.writeFloat(getValue().get());
    out.writeLong(edgeMap.size());
    edgeMap.forEachPair(new IntIntProcedure() {
      @Override
      public boolean apply(int destVertexId, int edgeValue) {
        try {
          out.writeInt(destVertexId);
          out.writeFloat(edgeValue);
        } catch (IOException e) {
          throw new IllegalStateException(
              "apply: IOException when not allowed", e);
        }
        return true;
      }
    });
    out.writeBoolean(isHalted());
  }

  /**
   * Helper iterable over the messages.
   */
  private static class UnmodifiableIntWritableIterable
    implements Iterable<IntWritable> {
    /** Backing store of messages */
    private final IntArrayList elementList;

    /**
     * Constructor.
     *
     * @param elementList Backing store of element list.
     */
    public UnmodifiableIntWritableIterable(
        IntArrayList elementList) {
      this.elementList = elementList;
    }

    @Override
    public Iterator<IntWritable> iterator() {
      return new UnmodifiableIntWritableIterator(
          elementList);
    }
  }

  /**
   * Iterator over the messages.
   */
  private static class UnmodifiableIntWritableIterator
      extends UnmodifiableIterator<IntWritable> {
    /** Double backing list */
    private final IntArrayList elementList;
    /** Offset into the backing list */
    private int offset = 0;

    /**
     * Constructor.
     *
     * @param elementList Backing store of element list.
     */
    UnmodifiableIntWritableIterator(IntArrayList elementList) {
      this.elementList = elementList;
    }

    @Override
    public boolean hasNext() {
      return offset < elementList.size();
    }

    @Override
    public IntWritable next() {
      return new IntWritable(elementList.get(offset++));
    }
  }
}
