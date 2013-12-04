package ml.grafos.okapi.partitioning;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import ml.grafos.okapi.examples.SimpleMasterComputeVertex;

import org.apache.giraph.Algorithm;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
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
    "This is a scalable graph partitioning algorithm that:\n"
    + "(a) Produces k-way balanced partitions.\n"
    + "(b) Minimizes the number of cut edges until convergence.\n"
    + "(c) Adapts to dynamic graph changes with minimum cost.\n"
)

public class Partitioning extends BasicComputation<IntWritable,
  IntWritable, IntWritable, IntMessageWrapper> {
  /** Keyword for Probability barrier for a vertex to migrate. */
  public static final String PROBABILITY = "partitioning.probability";
  /** Default value for Probability barrier for a vertex to migrate. */
  public static final float PROBABILITY_DEFAULT = 0.5f;
  /** Keyword for parameter regarding number of partitions. */
  public static final String NUM_PARTITIONS = "partitioning.num.partition";
  /** Default value for parameter regarding number of partitions. */
  public static final int NUM_PARTITIONS_DEFAULT = 1;
  /** Keyword for parameter setting the number of iterations. */
  public static final String ITERATIONS_KEYWORD = "partitioning.iterations";
  /** Default value for ITERATIONS. */
  public static final int ITERATIONS_DEFAULT = 10;
  /** Keyword for parameter setting the number of stabilization rounds. */
  public static final String STABILIZATION_KEYWORD =
    "partitioning.stabilization";
  /** Default value for stabilization rounds. */
  public static final int STABILIZATION_DEFAULT = 30;
  /** Keyword for parameter enabling delta caching. */
  public static final String DELTA_CACHING = "partitioning.delta.caching";
  /** Default value for parameter enabling delta caching. */
  public static final boolean DELTA_CACHING_DEFAULT = false;
  /** String prefix for aggregators summing the partitions capacity. */
  public static final String AGGREGATOR_CAPACITY_PREFIX = "AGG_CAP_";
  /** String prefix for aggregators summing the partitions demand. */
  public static final String AGGREGATOR_DEMAND_PREFIX = "AGG_DEM_";
  /** Name of aggregator counting the local edges. */
  public static final String AGGREGATOR_COUNT_LOCAL_EDGES = "AGG_COUNT";
  /** New object IntWritable with the value 1. */
  public static final IntWritable PLUS_ONE = new IntWritable(1);
  /** New object IntWritable with the value -1. */
  public static final IntWritable MINUS_ONE = new IntWritable(-1);
  /** Number used in the calculation of total capacity. */
  public static final double POINTTWO = 0.2;
  /** Counter of number of migrations. */
  private int countMigrations = 0;
  /** Initial value of vertex - Partition Id the vertex was assigned to. */
  private int initialValue = 0;

  // XXX ATTENTION XXX
  // This is maintained across different calls to compute()
  // but they are not maintained if the vertex is written to
  // disk or a failure happens.
  // To ensure it's maintained, we need to make it part of
  // the vertex value.
  /** Migrate to partition given in this variable. */
  private int migrate2partition = -1;
  // XXX OPTIMIZATION XXX
  // This is a similar case to the above.
  // We can add this to the master compute or have another aggregator
  // to check the number of stabilization rounds
  /** A counter of consecutive rounds with no migrations. */
  private int stabilizationRounds = 0;

  /**
   * Compute method.
   * @param messages Messages received
   */
  public final void compute(
      Vertex<IntWritable, IntWritable, IntWritable> vertex,
      final Iterable<IntMessageWrapper> messages) {
    int iterations = getContext().getConfiguration().getInt(
      ITERATIONS_KEYWORD, ITERATIONS_DEFAULT);
    int stabilization = getContext().getConfiguration().getInt(
      STABILIZATION_KEYWORD, STABILIZATION_DEFAULT);

    /* Halt Condition */
    if (stabilizationRounds == stabilization || getSuperstep() > iterations) {
      /*System.out.println("stabilizationRounds: " + stabilizationRounds +
        ", getSuperstep(): " + getSuperstep());*/
      vertex.voteToHalt();
      return;
    }

    /* Parameter for number of partitions */
    int numPartitions = getContext().getConfiguration().getInt(
      NUM_PARTITIONS, NUM_PARTITIONS_DEFAULT);
    /* Parameter for probability number */
    double probability = getContext().getConfiguration().getFloat(
      PROBABILITY, PROBABILITY_DEFAULT);
    /* Generator of random numbers */
    Random randomGenerator = new Random();

    // Recompute capacity every time because it's not maintained.
    int totalCapacity = (int) (getTotalNumVertices() / numPartitions);
    totalCapacity = totalCapacity + (int) (totalCapacity * POINTTWO) + 1;

    /* Superstep 0
     * - Initialize Vertex Value
     * - Compute totalCapacity
     * - Send its partition ID to corresponding aggregator
     */
    if (getSuperstep() == 0) {
      /*System.out.println("***** SS:" + getSuperstep() +
        ", vertexID: " + getId() + ", totalCapacity: " + totalCapacity); */
      initValue(vertex, numPartitions);
      setInitialValue(vertex);

      /* Send to capacity aggregator a PLUS_ONE signal */
      aggregate(AGGREGATOR_CAPACITY_PREFIX + vertex.getValue().get(), PLUS_ONE);
      advertisePartition(vertex);
      return;
    }

    if (getSuperstep() % 2 == 1) {
      /* Odd Supersteps: Show interest to migrate to a partition */
      /* Store in vertex edges the neighbors' Partition IDs */
      for (IntMessageWrapper message : messages) {
        /*System.out.println("  [RECEIVE] from " + message.getSourceId() +
          ", " + message.getMessage()); */
        vertex.setEdgeValue(message.getSourceId(), message.getMessage());
      }

      /* Count neighbors in each partition --> store in a hashmap */
      /* HashMap<PartitionID, num_neighbours_in_PartitionID> */
      HashMap<Integer, Integer> countNeigh =
        new HashMap<Integer, Integer>();
      /* HashMap<PartitionID, weight_2migrage_2PartitionID> */
      HashMap<Integer, Double> weightedPartition =
        new HashMap<Integer, Double>();

      /* Count number of neighbors in neighbor's partition */
      for (Edge<IntWritable, IntWritable> edge : vertex.getEdges()) {
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
      if (prob < probability) {
        /*
         * Calculate the weight of migration to each partition that has
         * neighbors
         */
        for (Map.Entry<Integer, Double> partition
          :weightedPartition.entrySet()) {
          int load = ((IntWritable)
            getAggregatedValue(AGGREGATOR_CAPACITY_PREFIX
              + partition.getKey())).get();
          int numNeighbors = vertex.getNumEdges();
          int numNeighborsInPartition = countNeigh.get(partition.getKey());
          double weight =
            (1d - load / (double) totalCapacity) * (numNeighborsInPartition
              / (double) numNeighbors);
          weightedPartition.put(partition.getKey(), weight);
        }
        migrate2partition = maxWeightedPartition(vertex, weightedPartition);
        if (migrate2partition != vertex.getValue().get()) {
          aggregate(AGGREGATOR_DEMAND_PREFIX + migrate2partition, PLUS_ONE);
        }
      }
      return;
    } // EoF Odd Supersteps

    if (messages.iterator().hasNext()) {
      throw new RuntimeException(
        "BUG: vertex " + vertex.getId() + " received message in even superstep");
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
    if (migrate2partition != vertex.getValue().get() && 
        migrate2partition != -1) {
      int load = ((IntWritable) getAggregatedValue(
          AGGREGATOR_CAPACITY_PREFIX + migrate2partition)).get();
      int availability = totalCapacity - load;
      int demand = ((IntWritable)
        getAggregatedValue(
          AGGREGATOR_DEMAND_PREFIX + migrate2partition)).get();
      double finalProbability = (double) availability / demand;
      if (randomGenerator.nextDouble() < finalProbability) {
        migrate(vertex, migrate2partition);
        advertisePartition(vertex);
      }
    }
    int localEdges = 0;
    for (Edge<IntWritable, IntWritable> edge : vertex.getEdges()) {
      if (edge.getValue().get() == vertex.getValue().get()) {
        localEdges++;
      }
    }
    aggregate(AGGREGATOR_COUNT_LOCAL_EDGES, new IntWritable(localEdges));
  } // EoF compute()

  /**
   * Create message, fill it with data and send it to neighbors.
   */
  public final void advertisePartition(
      Vertex<IntWritable, IntWritable, IntWritable> vertex) {
    IntMessageWrapper message = new IntMessageWrapper();
    message.setSourceId(vertex.getId());
    message.setMessage(vertex.getValue());
    sendMessageToAllEdges(vertex, message);
  }

  /**
   * Move a vertex from current partition to a new partition.
   *
   * @param pMigrate2partition     PartitionID to be migrated to
   */
  public final void migrate(
      Vertex<IntWritable, IntWritable, IntWritable> vertex, 
      final int pMigrate2partition) {
    // Remove vertex from current partition
    aggregate(AGGREGATOR_CAPACITY_PREFIX + vertex.getValue().get(), MINUS_ONE);
    // Add vertex to new partition
    aggregate(AGGREGATOR_CAPACITY_PREFIX + pMigrate2partition, PLUS_ONE);
    incMigrationsCounter();
    /*System.out.println("  [AGG_CAP_SEND] Migrate to " + migrate2partition);
    System.out.println("  [AGG_CAP_SEND] Remove from " + getValue().get());*/
    vertex.setValue(new IntWritable(pMigrate2partition));
  }

  /**
   * Return partition with maximum number of neighbors.
   *
   * @param weightedPartition HashMap<PartitionID, weight>
   *  Weight for each partition
   *
   * @return partitionID    Partition ID with the maximum weight
   */
  public final int maxWeightedPartition(
      Vertex<IntWritable, IntWritable, IntWritable> vertex, 
      final HashMap<Integer, Double> weightedPartition) {
    Map.Entry<Integer, Double> maxEntry = null;
    for (Map.Entry<Integer, Double> entry
      :
      weightedPartition.entrySet()) {
      if (maxEntry == null
        ||
        entry.getValue() > maxEntry.getValue()) {
        maxEntry = entry;
      } else {
        if (entry.getValue() == maxEntry.getValue()) {
          if (entry.getKey() == vertex.getValue().get()) {
            maxEntry = entry;
          }
        }
      } // End of else{maxEntry NOT null)
    } // End of weightedPartition.entrySet()
    return maxEntry.getKey();
  }

  /**
   * Initialize Vertex Value.
   * VertexValue = VertexID mod num_of_partitions
   *
   * @param numPartitions Number of Partitions
   */
  public final void initValue(
      Vertex<IntWritable, IntWritable, IntWritable> vertex, 
      final int numPartitions) {
    //Random randomGenerator = new Random();
    //int partition = randomGenerator.nextInt(numPartitions);
    //setValue(new IntWritable(partition));
    vertex.setValue(new IntWritable(vertex.getId().get() % numPartitions));
  }

  /**
   * Set the initial value of the vertex.
   */
  final void setInitialValue(
      Vertex<IntWritable, IntWritable, IntWritable> vertex) {
    initialValue = vertex.getValue().get();
  }

  /**
   * Increase the counter holding the number of migrations.
   */
  public final void incMigrationsCounter() {
    countMigrations++;
  }

  /**
   * Return the number of consecutive migrations.
   * @return countMigrations
   */
  final int getMigrationsCounter() {
    return countMigrations;
  }

  /**
   * Return the initial partition the vertex was assigned to.
   * @return initialValue
   */
  final int getInitialPartition() {
    return initialValue;
  }

  /**
   * MasterCompute used with {@link SimpleMasterComputeVertex}.
   */
  public static class MasterCompute extends DefaultMasterCompute {
    @Override
    public final void compute() {
      System.out.println("S: " + getSuperstep() + " Local/Total: "
        + getAggregatedValue(AGGREGATOR_COUNT_LOCAL_EDGES) + " / "
        + getTotalNumEdges());
    }
    @Override
    public final void initialize() throws InstantiationException,
    IllegalAccessException {
      // Create aggregators - one for each partition
      for (int i = 0;
          i < getContext().getConfiguration().getInt(NUM_PARTITIONS,
            NUM_PARTITIONS_DEFAULT); i++) {
        registerPersistentAggregator(AGGREGATOR_CAPACITY_PREFIX + i,
          IntSumAggregator.class);
        registerAggregator(AGGREGATOR_DEMAND_PREFIX + i,
          IntSumAggregator.class);
        registerAggregator(AGGREGATOR_COUNT_LOCAL_EDGES,
          IntSumAggregator.class);
      }
    } // EoF initialize()
  } // EoF class MasterCompute{}
}
