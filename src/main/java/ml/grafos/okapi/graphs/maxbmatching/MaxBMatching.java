/**
 * Copyright 2014 Grafos.ml
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ml.grafos.okapi.graphs.maxbmatching;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import ml.grafos.okapi.graphs.maxbmatching.MBMEdgeValue.State;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
import org.python.google.common.collect.Maps;

import com.google.common.collect.MinMaxPriorityQueue;

/**
 * Greedy algorithm for the Maximum B-Matching problem as described in G. De Francisci Morales, A. Gionis, M. Sozio 
 * "Social Content Matching in MapReduce" in * PVLDB: Proceedings of the VLDB Endowment, 4(7):460-469, 2011.
 * 
 * Given a weighted undirected graph, with integer capacities assigned to each vertex, the maximum b-matching problem
 * is to select a subgraph of maximum weight  * such that the number of edges incident to each vertex in the subgraph
 * does not exceed its capacity. This is a greedy algorithm that provides a 1/2-approximation guarantee.
 */
public class MaxBMatching extends BasicComputation<LongWritable, IntWritable, MBMEdgeValue, MBMMessage> {
    private static final Logger LOG = Logger.getLogger(MaxBMatching.class);

    @Override
    public void compute(Vertex<LongWritable, IntWritable, MBMEdgeValue> vertex, Iterable<MBMMessage> messages) {
        if (LOG.isDebugEnabled())
            debug(vertex);
        if (vertex.getValue().get() < 0)
            throw new AssertionError("Capacity should never be negative: " + vertex);
        else if (vertex.getValue().get() == 0) {
            // vote to halt if capacity reaches zero, tell neighbors to remove edges connecting to this vertex
            removeVertex(vertex);
            vertex.voteToHalt();
            return;
        } else {
            if (getSuperstep() > 0) {
                // accept intersection between proposed and received
                processUpdates(vertex, messages);
            }
            if (vertex.getValue().get() > 0) {
                // propose edges to neighbors in decreasing order by weight up to capacity
                sendUpdates(vertex);
            }
        }
    }

    private void sendUpdates(Vertex<LongWritable, IntWritable, MBMEdgeValue> vertex) {
        final MBMMessage proposeMsg = new MBMMessage(vertex.getId(), State.PROPOSED);

        // get top-capacity available edges by weight
        final int capacity = vertex.getValue().get();
        MinMaxPriorityQueue<Entry<LongWritable, MBMEdgeValue>> maxHeap = MinMaxPriorityQueue.orderedBy(new Comparator<Entry<LongWritable, MBMEdgeValue>>() {
            @Override
            public int compare(Entry<LongWritable, MBMEdgeValue> o1, Entry<LongWritable, MBMEdgeValue> o2) {
                return -1 * Double.compare(o1.getValue().getWeight(), o2.getValue().getWeight()); // reverse comparator, largest weight first
            }
        }).maximumSize(capacity).create();
        // prepare list of available edges
        for (Edge<LongWritable, MBMEdgeValue> e : vertex.getEdges()) {
            if (e.getValue().getState() == State.DEFAULT || e.getValue().getState() == State.PROPOSED) {
                maxHeap.add(Maps.immutableEntry(e.getTargetVertexId(), e.getValue()));
            }
        }

        if (maxHeap.isEmpty()) {
            // all remaining edges are INCLUDED, nothing else to do
            checkSolution(vertex.getEdges());
            vertex.voteToHalt();
        } else {
            // propose up to capacity
            while (!maxHeap.isEmpty()) {
                Entry<LongWritable, MBMEdgeValue> entry = maxHeap.removeFirst();
                vertex.getEdgeValue(entry.getKey()).setState(State.PROPOSED);
                sendMessage(entry.getKey(), proposeMsg);
            }
        }
    }

    private void processUpdates(Vertex<LongWritable, IntWritable, MBMEdgeValue> vertex, Iterable<MBMMessage> messages) throws AssertionError {
        Set<LongWritable> toRemove = new HashSet<LongWritable>();
        int numIncluded = 0;

        for (MBMMessage msg : messages) {
            MBMEdgeValue edgeValue = vertex.getEdgeValue(msg.getId());
            if (edgeValue == null) {
                // edge has already been removed, do nothing
                if (LOG.isDebugEnabled())
                    LOG.debug(String.format("Superstep %d Vertex %d: message for removed edge from vertex %d", getSuperstep(), vertex.getId().get(), msg
                            .getId().get()));
            } else {
                if (msg.getState() == State.PROPOSED && edgeValue.getState() == State.PROPOSED) {
                    edgeValue.setState(State.INCLUDED);
                    numIncluded++;
                } else if (msg.getState() == State.REMOVED) {
                    toRemove.add(msg.getId());
                }
            }
        }
        // update capacity
        vertex.getValue().set(vertex.getValue().get() - numIncluded);
        // remove edges locally
        for (LongWritable e : toRemove)
            vertex.removeEdges(e);

        if (LOG.isDebugEnabled())
            LOG.debug(String.format("Superstep %d Vertex %d: included %d edges, removed %d edges", getSuperstep(), vertex.getId().get(), numIncluded,
                    toRemove.size()));
    }

    private void removeVertex(Vertex<LongWritable, IntWritable, MBMEdgeValue> vertex) {
        Set<LongWritable> toRemove = new HashSet<LongWritable>();
        final MBMMessage removeMsg = new MBMMessage(vertex.getId(), State.REMOVED);

        for (Edge<LongWritable, MBMEdgeValue> e : vertex.getEdges()) {
            MBMEdgeValue edgeState = e.getValue();
            // remove remaining DEFAULT edges
            if (edgeState.getState() == State.DEFAULT) {
                sendMessage(e.getTargetVertexId(), removeMsg);
                toRemove.add(e.getTargetVertexId());
            }
        }
        for (LongWritable e : toRemove)
            vertex.removeEdges(e);
    }

    private void checkSolution(Iterable<Edge<LongWritable, MBMEdgeValue>> collection) {
        for (Edge<LongWritable, MBMEdgeValue> e : collection)
            if (e.getValue().getState() != State.INCLUDED)
                throw new AssertionError(String.format("All the edges in the matching should be %d, %d was %d", State.INCLUDED, e, e.getValue().getState()));
    }

    private void debug(Vertex<LongWritable, IntWritable, MBMEdgeValue> vertex) {
        LOG.debug(vertex);
        for (Edge<LongWritable, MBMEdgeValue> e : vertex.getEdges())
            LOG.debug(String.format("Edge(%d, %s)", e.getTargetVertexId().get(), e.getValue().toString()));
    }
}
