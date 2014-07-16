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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;

import ml.grafos.okapi.graphs.maxbmatching.MBMEdgeState.State;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;

/**
 * Greedy algorithm for Maximum B-Matching problem as described in G. De Francisci Morales, A. Gionis, M. Sozio "Social Content Matching in MapReduce" in PVLDB:
 * Proceedings of the VLDB Endowment, 4(7):460-469, 2011.
 * 
 * Given a weighted graph, with integer capacities assigned to each vertex, the maximum b-matching problem is to select a subgraph of maximum weight such that
 * the number of edges incident to each vertex in the subgraph does not exceed its capacity. This is a greedy algorithm that provides a 1/2 approximation
 * guarantee.
 */
public class MaxBMatching extends BasicComputation<LongWritable, IntWritable, MBMEdgeState, MBMMessage> {

    /* Logger */
    private static final Logger LOG = Logger.getLogger(MaxBMatching.class);

    @Override
    public void compute(Vertex<LongWritable, IntWritable, MBMEdgeState> vertex, Iterable<MBMMessage> messages) {
        /* Messages */
        final MBMMessage removeMsg = new MBMMessage(vertex.getId(), State.REMOVED);
        final MBMMessage proposeMsg = new MBMMessage(vertex.getId(), State.PROPOSED);

        if (vertex.getValue().get() < 0)
            throw new AssertionError("Capacity should never be negative");

        if (getSuperstep() > 0) {
            Set<LongWritable> toRemove = new HashSet<LongWritable>();
            int numAccepted = 0;
            // process messages: accept intersection between proposed and received
            for (MBMMessage msg : messages) {
                MBMEdgeState edgeState = vertex.getEdgeValue(msg.getId());
                if (edgeState == null)
                    throw new AssertionError("Null edge state: superstep=" + getSuperstep() + " vertex=" + vertex.getId() + " msgSource=" + msg.getId());
                if (msg.getState() == State.PROPOSED && edgeState.getState() == State.PROPOSED) {
                    edgeState.setState(State.INCLUDED);
                    vertex.getValue().set(vertex.getValue().get() - 1); // vertex.value--
                    numAccepted++;
                } else if (msg.getState() == State.REMOVED) {
                    edgeState.setState(State.REMOVED);
                    toRemove.add(msg.getId());
                }
            }
            for (LongWritable e : toRemove)
                vertex.removeEdges(e);
            if (LOG.isInfoEnabled())
                LOG.info(String.format("Superstep %d: accepted %d edges", getSuperstep(), numAccepted));
        }

        // vote to stop if capacity reached zero, tell neighbors to remove edges connecting to this vertex
        if (vertex.getValue().get() == 0) {
            for (Edge<LongWritable, MBMEdgeState> e : vertex.getEdges()) {
                MBMEdgeState edgeState = e.getValue();
                if (edgeState.getState() != State.INCLUDED) { // remove DEFAULT and PROPOSED edges
                    sendMessage(e.getTargetVertexId(), removeMsg);
                    sendMessage(vertex.getId(), new MBMMessage(e.getTargetVertexId(), State.REMOVED));
                }
            }
            vertex.voteToHalt();
            return;
        }

        // prepare list of available edges
        ArrayList<Edge<LongWritable, MBMEdgeState>> arrayList = new ArrayList<Edge<LongWritable, MBMEdgeState>>();
        for (Edge<LongWritable, MBMEdgeState> e : vertex.getEdges()) {
            if (e.getValue().getState() == State.DEFAULT || e.getValue().getState() == State.PROPOSED)
                arrayList.add(e);
        }

        // nothing else to do
        if (arrayList.size() == 0) {
            check(vertex.getEdges());
            vertex.voteToHalt();
        } else {
            // sort it by decreasing weight
            Collections.sort(arrayList, new Comparator<Edge<LongWritable, MBMEdgeState>>() {
                @Override
                public int compare(Edge<LongWritable, MBMEdgeState> e1, Edge<LongWritable, MBMEdgeState> e2) {
                    return -1 * Double.compare(e1.getValue().getWeight(), e2.getValue().getWeight());
                }
            });
            // propose up to capacity
            int numToPropose = (int) Math.min(vertex.getValue().get(), arrayList.size());
            for (int i = 0; i < numToPropose; i++) {
                Edge<LongWritable, MBMEdgeState> edge = arrayList.get(i);
                edge.getValue().setState(State.PROPOSED);
                sendMessage(edge.getTargetVertexId(), proposeMsg);
            }
        }
    }

    private void check(Iterable<Edge<LongWritable, MBMEdgeState>> collection) {
        for (Edge<LongWritable, MBMEdgeState> e : collection)
            if (e.getValue().getState() != State.INCLUDED)
                throw new AssertionError(String.format("All the edges in the matching should be {1}, {2} was {3}", State.INCLUDED, e, e.getValue().getState()));
    }
}
