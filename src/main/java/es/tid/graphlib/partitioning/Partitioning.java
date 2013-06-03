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

@Algorithm(
  name = "Adaptive Partitioning for Large-Scale Dynamic Graphs",
  description =
    "This is a scalable graph partitioning algorithm that:\n" +
    "(a) Produces k-way balanced partitions.\n" +
    "(b) Minimizes the number of cut edges until convergence.\n" +
    "(c) Adapts to dynamic graph changes with minimum cost.\n"
)

public class Partitioning extends Vertex<IntWritable,
  IntWritable, IntWritable, IntMessageWrapper> {
  /** Keyword for Probability barrier for a vertex to migrate */
  public static final String PROBABILITY = "partitioning.probability";
  /** Default value for Probability barrier for a vertex to migrate */
  public static final float PROBABILITY_DEFAULT = 0.5f;
  /** Keyword for parameter regarding number of partitions*/
  public static final String NUM_PARTITIONS = "partitioning.num.partition";
  /** Default value for parameter regarding number of partitions */
  public static final int NUM_PARTITIONS_DEFAULT = 1;
  /** Keyword for parameter enabling delta caching */
  public static final String DELTA_CACHING = "partitioning.delta.caching";
  /** Default value for parameter enabling delta caching */
  public static final boolean DELTA_CACHING_DEFAULT = false;
  /** String prefix for aggregators summing the partitions capacity */
  public static final String AGGREGATOR_CAPACITY_PREFIX = "AGG_CAP_";
  /** String prefix for aggregators summing the partitions demand */
  public static final String AGGREGATOR_DEMAND_PREFIX = "AGG_DEM_";
  /** New object IntWritable with the value 1 */
  public static final IntWritable PLUS_ONE = new IntWritable(1);
  /** New object IntWritable with the value -1 */
  public static final IntWritable MINUS_ONE = new IntWritable(-1);
  /** Iterations */
  public static final int ITERATIONS = 60;
  /** Counter of number of migrations */
  private int countMigrations = 0;
  /** Initial value of vertex - Partition Id the vertex was assigned to */
  private int initialValue = 0;

  // XXX ATTENTION XXX
  // This is maintained across different calls to compute()
  // but they are not maintained if the vertex is written to
  // disk or a failure happens.
  // To ensure it's maintained, we need to make it part of
  // the vertex value.
  /** Migrate to partition given in this variable */
  private int migrate2partition = -1;
  // XXX OPTIMIZATION XXX
  // This is a similar case to the above.
  // We can add this to the master compute or have another aggregator
  // to check the number of stabilization rounds
  /** A counter of consecutive rounds with no migrations */
  private int stabilizationRounds = 0;

  /**
   * Compute method
   * @param messages Messages received
   */
  public void compute(Iterable<IntMessageWrapper> messages) {

    /* Halt Condition */
    if (stabilizationRounds == 30 || getSuperstep() > ITERATIONS) {
      /*System.out.println("stabilizationRounds: " + stabilizationRounds +
        ", getSuperstep(): " + getSuperstep());*/
      voteToHalt();
      return;
    }

    /* Parameter for number of partitions */
    int numPartitions = getContext().getConfiguration().getInt(NUM_PARTITIONS,
      NUM_PARTITIONS_DEFAULT);
    /* Parameter for probability number */
    double probability = getContext().getConfiguration().getFloat(PROBABILITY,
      PROBABILITY_DEFAULT);
    /* Generator of random numbers */
    Random randomGenerator = new Random();

    // Recompute capacity every time because it's not maintained.
    int totalCapacity = (int) (getTotalNumVertices() / numPartitions);
    totalCapacity = totalCapacity + (int) (totalCapacity * 0.2) + 1;

    /* Superstep 0
     * - Initialize Vertex Value
     * - Compute totalCapacity
     * - Send its partition ID to corresponding aggregator
     */
    if (getSuperstep() == 0) {
      /*System.out.println("***** SS:" + getSuperstep() +
        ", vertexID: " + getId() + ", totalCapacity: " + totalCapacity); */
      initValue(numPartitions);
      setInitialValue();

      /* Send to capacity aggregator a PLUS_ONE signal */
      aggregate(AGGREGATOR_CAPACITY_PREFIX + getValue().get(), PLUS_ONE);
      advertisePartition();
      return;
    }

    if (getSuperstep() % 2 == 1) {
      /* Odd Supersteps: Show interest to migrate to a partition */
      /*System.out.println("***** SS:" + getSuperstep() + ", vertexID: " +
        getId() + ", PartitionID: " + getValue() + " REQUEST a migration");
      System.out.println("AGG_CAP[0]:" +
        getAggregatedValue(AGGREGATOR_CAPACITY_PREFIX + 0) + ", AGG_CAP[1]: " +
          getAggregatedValue(AGGREGATOR_CAPACITY_PREFIX + 1));*/
      /* Store in vertex edges the neighbors' Partition IDs */
      for (IntMessageWrapper message : messages) {
        /*System.out.println("  [RECEIVE] from " + message.getSourceId() +
          ", " + message.getMessage()); */
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
        if (currCount == null) {
          countNeigh.put(edge.getValue().get(), 1);
          weightedPartition.put(edge.getValue().get(), 0d);
        } else {
          countNeigh.put(edge.getValue().get(), currCount + 1);
        }
      } // EoF edges iteration

      /* Introduce random factor for migrating or not */
      migrate2partition = -1;
      /* Allow migration only with certain probability */
      double prob = randomGenerator.nextDouble();
      /* System.out.println("prob: " + prob); */
      if (prob < probability) {
        /*
         * Calculate the weight of migration to each partition that has
         * neighbors
         */
        for (Map.Entry<Integer, Double> partition:
          weightedPartition.entrySet()){
          System.out.println("getKey:" + partition.getKey());
        }
        for (Map.Entry<Integer, Double> partition :
          weightedPartition.entrySet()) {
          System.out.println("I want the value from " + AGGREGATOR_CAPACITY_PREFIX + partition.getKey());
          int load = ((IntWritable)
            getAggregatedValue(AGGREGATOR_CAPACITY_PREFIX +
              partition.getKey())).get();
          int numNeighbors = getNumEdges();
          int numNeighborsInPartition = countNeigh.get(partition.getKey());
          double weight =
            (1d - load / (double) totalCapacity) * (numNeighborsInPartition /
              (double) numNeighbors);
          /*System.out.println("CALC_WEIGHT: key:" + partition.getKey() +
            ", weight = 1 - load(" + load + ") / totalCapacity(" +
              totalCapacity + ") *" + "(neigh_part / tot_neigh = " +
                ((double) numNeighborsInPartition / numNeighbors) + "): " +
                  weight); */
          weightedPartition.put(partition.getKey(), weight);
        }
        migrate2partition = maxWeightedPartition(weightedPartition);
        if (migrate2partition != getValue().get()) {
          aggregate(AGGREGATOR_DEMAND_PREFIX + migrate2partition, PLUS_ONE);
          /*System.out.println("  [AGG_DEM_SEND] I want to migrate to " +
            migrate2partition); */
        }
      } else {
        /*System.out.println("No migration this time! (probability small)");*/
        //XXX Can the vertex voteToHalt() at this point?
        //voteToHalt();
      }

      return;
    } // EoF Odd Supersteps

    if (messages.iterator().hasNext()) {
      throw new RuntimeException(
        "BUG: vertex " + getId() + " received message in even superstep");
    }

    /* Even Supersteps: Migrate to partition */
    int noMigrations = 0;
    for (int i = 0; i < numPartitions; i++) {
      if (((IntWritable) getAggregatedValue(
          AGGREGATOR_DEMAND_PREFIX + i)).get() == 0) {
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
    /*System.out.println("StabilizationRounds = " + stabilizationRounds); */
    if (migrate2partition != getValue().get() && migrate2partition != -1) {
      /*System.out.print("***** SS:" + getSuperstep() + ", vertexID: " + getId() +
        ", from " + getValue() + " want to MIGRATE to: " + migrate2partition); */
      System.out.println("I want to MIGRATE from " + AGGREGATOR_CAPACITY_PREFIX + getValue()
          + " to " + AGGREGATOR_CAPACITY_PREFIX + migrate2partition);  
      int load = ((IntWritable) getAggregatedValue(
          AGGREGATOR_CAPACITY_PREFIX + migrate2partition)).get();
      int availability = totalCapacity - load;
      int demand = ((IntWritable)
        getAggregatedValue(AGGREGATOR_DEMAND_PREFIX +
          migrate2partition)).get();
      double finalProbability = (double) availability / demand;
      if (randomGenerator.nextDouble() < finalProbability) {
        /*System.out.println(" --> SUCCESS! (availability/demand=" +
          finalProbability + ")"); */
        migrate(migrate2partition);
        advertisePartition();
      } else {
        /*System.out.println(" --> FAIL! (availability/demand=" +
          finalProbability + ")"); */
      }
    }
  } // EoF compute()

  /**
   * Create message, fill it with data and send it to neighbors
   */
  public void advertisePartition() {
    /* Send Message to all neighbors*/
    for (Edge<IntWritable, IntWritable> edge : getEdges()) {
      /* Create a message and wrap together the source id and the message */
      IntMessageWrapper message = new IntMessageWrapper();
      message.setSourceId(getId());
      message.setMessage(getValue());
      sendMessage(edge.getTargetVertexId(), message);
      /*System.out.println("  [SEND] to " + edge.getTargetVertexId() +
        " PartitionID: " + message.getMessage()); */
    } // End of for each edge
  }

  /**
   * Move a vertex from current partition to a new partition
   *
   * @param migrate2partition     PartitionID to be migrated to
   */
  public void migrate(int migrate2partition) {
    // Remove vertex from current partition
    aggregate(AGGREGATOR_CAPACITY_PREFIX + getValue().get(), MINUS_ONE);
    // Add vertex to new partition
    aggregate(AGGREGATOR_CAPACITY_PREFIX + migrate2partition, PLUS_ONE);
    incMigrationsCounter();
    /*System.out.println("  [AGG_CAP_SEND] Migrate to " + migrate2partition);
    System.out.println("  [AGG_CAP_SEND] Remove from " + getValue().get());*/
    setValue(new IntWritable(migrate2partition));
  }

  /**
   * Return partition with maximum number of neighbors
   *
   * @param weightedPartition HashMap<PartitionID, weight>
   *  Weight for each partition
   *
   * @return partitionID    Partition ID with the maximum weight
   */
  public int maxWeightedPartition(HashMap<Integer,
    Double> weightedPartition) {
    Map.Entry<Integer, Double> maxEntry = null;
    /*System.out.println("weightedPartitions: " + weightedPartition.toString()); */
    for (Map.Entry<Integer, Double> entry :
      weightedPartition.entrySet()) {
      if (maxEntry == null ||
        entry.getValue() > maxEntry.getValue()) {
        /*if (maxEntry != null) {
          System.out.println("entry: " + entry.toString() + " > maxEntry: " +
            maxEntry.toString());
        } else {
          System.out.println("entry: " + entry.toString());
        }*/
        maxEntry = entry;
      } else {
        if (entry.getValue() == maxEntry.getValue()){
          if (entry.getKey() == getValue().get()) {
            maxEntry = entry;
            /*System.out.println("entry: " + entry.toString() +
              "=currentPartition= maxEntry: " + maxEntry.toString()); */
          }
        }
      }
    }
    /*System.out.println("maxWeightedPartition: " + maxEntry.toString()); */
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
    //int partition = randomGenerator.nextInt(numPartitions);
    //setValue(new IntWritable(partition));
    setValue(new IntWritable(getId().get() % numPartitions));
  }

  /**
   * Set the initial value of the vertex
   */
  void setInitialValue() {
    initialValue = getValue().get();
  }

  /**
   * Increase the counter holding the number of migrations
   */
  public void incMigrationsCounter() {
    countMigrations++;
  }

  /**
   * Return the number of consecutive migrations
   * @return countMigrations
   */
  int getMigrationsCounter() {
    return countMigrations;
  }

  /**
   * Return the initial partition the vertex was assigned to
   * @return initialValue
   */
  int getInitialPartition() {
    return initialValue;
  }

  /**
   * MasterCompute used with {@link SimpleMasterComputeVertex}.
   */
  public static class MasterCompute extends DefaultMasterCompute {
    @Override
    public void initialize() throws InstantiationException,
    IllegalAccessException {
      // Create aggregators - one for each partition
      for (int i = 0;
          i < getContext().getConfiguration().getInt(NUM_PARTITIONS,
            NUM_PARTITIONS_DEFAULT); i++) {
        registerPersistentAggregator(AGGREGATOR_CAPACITY_PREFIX + i,
          IntSumAggregator.class);
        registerAggregator(AGGREGATOR_DEMAND_PREFIX + i,
          IntSumAggregator.class);
      }
    } // EoF initialize()
  } // EoF class MasterCompute{}
}
