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
  public static final String PROBABILITY = "partitioning.probability";
  public static final float PROBABILITY_DEFAULT = 0.5f;
  public static final String NUM_PARTITIONS = "partitioning.num.partition";
  public static final int NUM_PARTITIONS_DEFAULT = 1;
  public static final String DELTA_CACHING = "partitioning.delta.caching";
  public static final boolean DELTA_CACHING_DEFAULT = false;
  public static final String AGGREGATOR_CAPACITY_PREFIX = "AGG_CAP_";
  public static final String AGGREGATOR_DEMAND_PREFIX = "AGG_DEM_";
  public static final IntWritable PLUS_ONE = new IntWritable(1);
  public static final IntWritable MINUS_ONE = new IntWritable(-1);
  /** Iterations */
  public static final int ITERATIONS = 30;
  /** Counter of number of migrations */
  public int countMigrations = 0;
  public int initialValue = 0;

  // XXX ATTENTION XXX
  // This is maintained across different calls to compute()
  // but they are not maintained if the vertex is written to
  // disk or a failure happens. 
  // To ensure it's maintained, we need to make it part of
  // the vertex value.
  private int migrate2partition = -1;
  // XXX OPTIMIZATION XXX
  // This is a similar case to the above.
  // We can add this to the master compute or have another aggregator
  // to check the number of stabilization rounds
  private int stabilizationRounds = 0;
  public void compute(Iterable<IntMessageWrapper> messages) {

    /* Halt Condition */
    if (stabilizationRounds == 30 || getSuperstep() > ITERATIONS) {
      System.out.println("stabilizationRounds: " + stabilizationRounds
        + ", getSuperstep(): " + getSuperstep());
      voteToHalt();
      return;
    }

    /* Parameter for number of partitions */
    int numPartitions =getContext().getConfiguration().getInt(NUM_PARTITIONS,
      NUM_PARTITIONS_DEFAULT);
    /* Parameter for probability number */
    double probability = getContext().getConfiguration().getFloat(PROBABILITY,
      PROBABILITY_DEFAULT);
    /* Flag for checking if delta caching is enabled */
    boolean deltaFlag = getContext().getConfiguration().getBoolean(
      DELTA_CACHING, DELTA_CACHING_DEFAULT);
    /* Generator of random numbers */
    Random randomGenerator = new Random();
    //double finalProbability = 0d;

    // Recompute capacity every time because it's not maintained.
    int CAPACITY = (int) (getTotalNumVertices() / numPartitions);
    CAPACITY = CAPACITY + (int)(CAPACITY * 0.2);

    /* Superstep 0
     * - Initialize Vertex Value
     * - Compute CAPACITY
     * - Send its partition ID to corresponding aggregator
     */
    if (getSuperstep() == 0) {
      System.out.println("***** SS:" + getSuperstep() + ", vertexID: " + getId() + ", CAPACITY: " + CAPACITY);
      initValue(numPartitions);
      initialValue = getValue().get();
      /* Send to capacity aggregator a PLUS_ONE signal */
      aggregate(AGGREGATOR_CAPACITY_PREFIX + getValue().get(), PLUS_ONE);
      advertisePartition();
      return;
    }
    //System.out.println("partitionID: " + getValue());

    if (getSuperstep() % 2 == 1) {
      /* Odd Supersteps: Show interest to migrate to a partition */
      System.out.println("***** SS:" + getSuperstep() + ", vertexID: " + getId() +
        ", PartitionID: " + getValue() +" REQUEST a migration");
      System.out.println("AGG_CAP[0]:" + getAggregatedValue(AGGREGATOR_CAPACITY_PREFIX + 0)
        + ", AGG_CAP[1]: " + getAggregatedValue(AGGREGATOR_CAPACITY_PREFIX + 1));
      //countNeigh.clear();
      /* Store in vertex edges the neighbors' Partition IDs */
      for (IntMessageWrapper message : messages) {
        System.out.println("  [RECEIVE] from " + message.getSourceId()
          + ", " + message.getMessage());
        setEdgeValue(message.getSourceId(), message.getMessage());
      }
      
      /* Count neighbors in each partition --> store in a hashmap */
      /* HashMap<PartitionID, num_neighbours_in_PartitionID> */
      HashMap<Integer, Integer> countNeigh =
        new HashMap<Integer, Integer>();
      /* HashMap<PartitionID, weight_2migrage_2PartitionID> */
      HashMap<Integer, Double> weightedPartition =
        new HashMap<Integer, Double>();

      /* Count number of neighbors in neighbor's partition */
      for (Edge<IntWritable, IntWritable> edge : getEdges()) {
        Integer currCount = countNeigh.get(edge.getValue().get());
        if (currCount==null) {
          countNeigh.put(edge.getValue().get(), 1);
          weightedPartition.put(edge.getValue().get(), 0d);
        } else {
          countNeigh.put(edge.getValue().get(), currCount+1);
        }
      } // EoF edges iteration

      /* Introduce random factor for migrating or not */
      migrate2partition = -1;
      /* Allow migration only with certain probability */
      double prob = randomGenerator.nextDouble();
      System.out.println("prob: " + prob);
      if (prob < probability) {
        /*
         * Calculate the weight of migration to each partition that has
         * neighbors
         */
        for (Map.Entry<Integer, Double> partition :
          weightedPartition.entrySet()) {
          int load = ((IntWritable)
            getAggregatedValue(AGGREGATOR_CAPACITY_PREFIX +
              partition.getKey())).get();
          int N_tot = getNumEdges();
          int N_part = countNeigh.get(partition.getKey());
          double weight = 
            (1d - load / (double) CAPACITY) * (N_part / (double) N_tot);
          System.out.println("CALC_WEIGHT: key:" + partition.getKey() +
            ", weight=1-load("+load+")/CAPACITY("+CAPACITY+") *" +
            "(neigh_part/tot_neigh="+
            ((double)(N_part) / N_tot) +"): "+ weight);
          weightedPartition.put(partition.getKey(), weight);
        }
        migrate2partition = maxWeightedPartition(weightedPartition);
        if (migrate2partition != getValue().get()) {
          aggregate(AGGREGATOR_DEMAND_PREFIX + migrate2partition, PLUS_ONE);
          System.out.println("  [AGG_DEM_SEND] I want to migrate to " + migrate2partition);
        }
      } else {
        System.out.println("No migration this time! (probability small)");
        
        //XXX Can the vertex voteToHalt() at this point?
        //voteToHalt();
      }

      return;
    } // EoF Odd Supersteps 

    if (messages.iterator().hasNext()) {
      throw new RuntimeException(
        "BUG: vertex "+getId()+" received message in even superstep");
    }
    
    /* Even Supersteps: Migrate to partition */
    int noMigrations=0;
    for (int i=0; i<numPartitions; i++) {
      if (((IntWritable)getAggregatedValue(
        AGGREGATOR_DEMAND_PREFIX+i)).get() == 0){
        noMigrations++;
      } else {
        break;
      }
    }
    if (noMigrations == numPartitions) {
      stabilizationRounds++;
    } else {
      stabilizationRounds = 0;
    }
    System.out.println("StabilizationRounds = " + stabilizationRounds);
    if (migrate2partition != getValue().get() && migrate2partition != -1) {
      System.out.print("***** SS:" + getSuperstep() + ", vertexID: " + getId() +
        ", from " + getValue() +" want to MIGRATE to: " + migrate2partition);
      int load = ((IntWritable)getAggregatedValue(
        AGGREGATOR_CAPACITY_PREFIX+migrate2partition)).get();
      int availability = CAPACITY - load;
      int demand = ((IntWritable)
        getAggregatedValue(AGGREGATOR_DEMAND_PREFIX +
          migrate2partition)).get();
      double finalProbability = (double) availability / demand;
      if (randomGenerator.nextDouble() < finalProbability) {
        System.out.println(" --> SUCCESS! (availability/demand=" + finalProbability + ")");
        migrate(migrate2partition);
        advertisePartition();
      } else {
        System.out.println(" --> FAIL! (availability/demand=" + finalProbability + ")");
      }
    }


    /* Send to neighbors: vertex residual capacity */

  } // EoF compute()

  /**
   * Send message to neighbors
   *
   * @param resCapacity     Residual Capacity
   */
  public void advertisePartition() {
    /* Send Message to all neighbors*/
    for (Edge<IntWritable, IntWritable> edge : getEdges()) {
      /* Create a message and wrap together the source id and the message */
      IntMessageWrapper message = new IntMessageWrapper();
      message.setSourceId(getId());
      message.setMessage(getValue());
      sendMessage(edge.getTargetVertexId(), message);
      System.out.println("  [SEND] to " + edge.getTargetVertexId() +
        " PartitionID: " + message.getMessage());
    } // End of for each edge
  }

  /**
   * Move a vertex from current partition to a new partition
   *
   * @param migrate2partition     PartitionID to be migrated to
   */
  public void migrate(int migrate2partition){
    // Remove vertex from current partition
    aggregate(AGGREGATOR_CAPACITY_PREFIX + getValue().get(), MINUS_ONE);
    // Add vertex to new partition
    aggregate(AGGREGATOR_CAPACITY_PREFIX + migrate2partition, PLUS_ONE);
    countMigrations+=1;
    System.out.println("  [AGG_CAP_SEND] Migrate to " + migrate2partition);
    System.out.println("  [AGG_CAP_SEND] Remove from " + getValue().get());
    setValue(new IntWritable(migrate2partition));
  }

  /**
   * Return partition with maximum number of neighbors
   *
   * @param HashMap<PartitionID, weight> weightedPart
   *  Weight for each partition
   *
   * @return partitionID    Partition ID with the maximum weight
   */
  public int maxWeightedPartition(HashMap<Integer,
    Double> weightedPartition) {
    Map.Entry<Integer, Double> maxEntry = null;
    System.out.println("weightedPartitions: " + weightedPartition.toString());
    for (Map.Entry<Integer, Double> entry :
      weightedPartition.entrySet()) {
      if (maxEntry == null ||
        entry.getValue() > maxEntry.getValue()) {
        if (maxEntry != null)
          System.out.println("entry: " + entry.toString() + " > maxEntry: " + maxEntry.toString());
        else
          System.out.println("entry: " + entry.toString());
        maxEntry = entry;
      } else
        if (entry.getValue() == maxEntry.getValue()){
          if (entry.getKey() == getValue().get()) {
            maxEntry = entry;
            System.out.println("entry: " + entry.toString() + "=currentPartition= maxEntry: " + maxEntry.toString());

          }
        }
    }
    System.out.println("maxWeightedPartition: " + maxEntry.toString());
    return maxEntry.getKey();
  }

  /**
   * Initialize Vertex Value with the equation:
   * VertexValue = VertexID mod num_of_partitions
   *
   * @param numPartitions       Number of Partitions
   */
  public void initValue(int numPartitions) {
    Random randomGenerator = new Random();
    int partition = randomGenerator.nextInt(2);
    setValue(new IntWritable(partition));
    //setValue(new IntWritable(getId().get() % numPartitions));
  }

  /**
   * MasterCompute used with {@link SimpleMasterComputeVertex}.
   */
  public static class MasterCompute extends DefaultMasterCompute {
    @Override
    public void initialize() throws InstantiationException,
    IllegalAccessException {
      // Create aggregators - one for each partition
      for (int i=0; i<getContext().getConfiguration().getInt(NUM_PARTITIONS,
        NUM_PARTITIONS_DEFAULT); i++) {
        registerPersistentAggregator(AGGREGATOR_CAPACITY_PREFIX + i,
          IntSumAggregator.class);
        registerAggregator(AGGREGATOR_DEMAND_PREFIX + i,
          IntSumAggregator.class);
      }
    } // EoF initialize()
  } // EoF class MasterCompute{}
}
