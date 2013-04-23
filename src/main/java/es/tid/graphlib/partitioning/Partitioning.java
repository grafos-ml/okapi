package es.tid.graphlib.partitioning;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.IntWritable;

/**
 * Demonstrates the Pregel-based implementation of an adaptive partitioning
 * algorithm for Large-Scale Dynamic Graphs.
 */

@Algorithm(name = "Adaptive Partitioning for Large-Scale Dynamic Graphs",
  description = "This is a scalable graph partitioning algorithm that: "
    + "(a) Produces k-way balanced partitions. "
    + "(b) Minimizes the number of cut edges until convergence. "
    + "(c) Adapts to dynamic graph changes with minimum cost. ")

public class Partitioning extends Vertex<IntWritable,
  IntWritable, IntWritable, IntMessageWrapper> {

  /** Iterations */
  public static final int ITERATIONS = 30;
  /** Prefix for each aggregator's name */
  public static final String AGGREGATOR_PREFIX = "AGG_";
  public static final IntWritable PLUS_ONE = new IntWritable(1);
  public static final IntWritable MINUS_ONE = new IntWritable(-1);
  /** Aggregator to get values from the workers to the master */
  public static final String AGG = "capacity.aggregator";
  
  /** Total Capacity for each partition */
  public static int CAPACITY = 0;

  /** Vertex Residual Capacity */
  public int resCapacity = 0;

  /** Parameter for number of partitions */
  public final int numPartitions = getContext().getConfiguration().getInt("partitions", 1);
  /** Parameter for probability number */
  public final double probability = getContext().getConfiguration().getFloat("probability", (float) 0.5);
  public int countMigrations = 0;
  public void compute(Iterable<IntMessageWrapper> messages) {
    // Initialize Vertex Value & Compute CAPACITY
    if (getSuperstep() == 0) {
      initValue();
      CAPACITY = (int) (getTotalNumVertices() / numPartitions + 
        getTotalNumVertices() * 0.1);
      int numVertices = getAggregatedValue(AGGREGATOR_PREFIX + getValue().get());
      // Vertex residual capacity
      resCapacity = CAPACITY - numVertices;
      sendMessage(resCapacity);

      // Send to aggregator its value - Partition ID
      aggregate(AGGREGATOR_PREFIX + getValue().get(), PLUS_ONE);
    }

    if (getSuperstep() > 0) {
      // HashMap<PartitionID, num_total_possible_migrations_to_PartitionID>
      HashMap<IntWritable, IntWritable> numMigrations =
        new HashMap<IntWritable, IntWritable>();
      // HashMap<PartitionID, num_neighbours_in_PartitionID>
      HashMap<IntWritable, IntWritable> countNeigh =
        new HashMap<IntWritable, IntWritable>();

      // Go through my neighbours
      /** For each message */
      for (IntMessageWrapper message : messages) {
        // If numMigrations is not already set for this partition, then add it
        if (!numMigrations.containsKey(message.getSourceId())) {
          // Neighbour's residual capacity
          int numNeighVertices =
            getAggregatedValue(AGGREGATOR_PREFIX+message.getSourceId().get());
          int resNeighCapacity = CAPACITY - numNeighVertices;
          // Compute amount of available migrations
          numMigrations.put(message.getSourceId(),
            new IntWritable(resNeighCapacity / (numPartitions-1)));
          int rest = resNeighCapacity % (numPartitions-1);
          // Introduce random factor for migrating or not
          Random randomGenerator = new Random();
          boolean[] flags = new boolean[numPartitions];
          for (int i=0; i<flags.length; i++) {
            flags[i] = false;
          }
          for (int i=0; i<rest; i++) {
            do{
              randomGenerator.nextInt(numPartitions);
              resNeighCapacity+=1;
              if (flags[i] == false) {
                flags[i] = true;
                break;
              }
            } while(flags[i]==true);
          }
        } // EoF HashMap listing

        // Count number of neighbors in neighbor's partition
        if (!countNeigh.containsKey(message.getSourceId())) {
          countNeigh.put(message.getSourceId(), new IntWritable(1));
        } else {
          countNeigh.put(message.getSourceId(),
            new IntWritable(countNeigh.get(message.getSourceId()).get()+1));
        }

        // Introduce random factor for migrating or not
        Random randomGenerator = new Random();
        int migrate2partition = 0;
        if (randomGenerator.nextDouble() < probability) {
          do {
            migrate2partition = maxNeighbors(countNeigh);
            // If partition to migrate is not the current one
            if (migrate2partition != getValue().get()) {
              if (numMigrations.get(migrate2partition).get() == 0) {
                countNeigh.get(migrate2partition).set(0);
              } else {
                migrate(migrate2partition);
              }
            }
          } while (numMigrations.get(migrate2partition).get() == 0);
        }
      } // EoF Messages
    } // EoF getSuperstep() > 1
  } // EoF compute()

  /** Send message to neighbors */
  public void sendMessage(int resCapacity) {
    /** Send to all neighbors a message */
    for (Edge<IntWritable, IntWritable> edge : getEdges()) {
      /** Create a message and wrap together the source id and the message */
      IntMessageWrapper message = new IntMessageWrapper();
      message.setSourceId(getId());
      message.setMessage(new IntWritable(resCapacity));
      
      sendMessage(edge.getTargetVertexId(), message);
      /*
       * System.out.println("  [SEND] to " + edge.getTargetVertexId() +
       * " (partitionID: " + edge.getValue());
       */} // End of for each edge
  }

  /** Move a vertex from current partition to a new partition */
  public void migrate(int migrate2partition){
    // Remove vertex from current partition
    aggregate(AGGREGATOR_PREFIX + getValue().get(), MINUS_ONE);
    // Add vertex to new partition
    aggregate(AGGREGATOR_PREFIX + migrate2partition, MINUS_ONE);
    countMigrations+=1;
  }

  /**
   * Return partition with maximum number of neighbors
   * @param HashMap<IntWritable, IntWritable> countNeigh
   * @return IntWritable partitionID
   */
  public int maxNeighbors(HashMap<IntWritable, IntWritable> countNeigh) {
    Map.Entry<IntWritable, IntWritable> maxEntry = null;
    for (Map.Entry<IntWritable, IntWritable> entry : countNeigh.entrySet()) {
      if (maxEntry == null ||
        entry.getValue().compareTo(maxEntry.getValue()) > 0) {
          maxEntry = entry;
      } else if (entry.getValue().compareTo(maxEntry.getValue()) == 0){
          if (entry.getKey() == getValue()) {
            maxEntry = entry;
          }
      }
    }
    return maxEntry.getKey().get();
  }

  /**
   * Initialize Vertex Value
   * VertexValue = VertexID mod num_of_partitions
   */
  public void initValue() {
    getValue().set(getId().get() % numPartitions);
    System.out.println("[INIT] value: " + getValue());
  }

  /**
   * MasterCompute used with {@link SimpleMasterComputeVertex}.
   */
  public class MasterCompute extends DefaultMasterCompute {

    @Override
    public void initialize() throws InstantiationException,
      IllegalAccessException {
      // Create aggregators - one for each partition
      for (int i=0; i<numPartitions; i++) {
        registerPersistentAggregator(AGGREGATOR_PREFIX+i, IntSumAggregator.class);
      }
    } // EoF initialize()
  } // EoF class MasterCompute{}
}
