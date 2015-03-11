/**	
 * Inria Sophia Antipolis - Team COATI - 2015 
 * @author Flavian Jacquot
 * 
 */
package ml.grafos.okapi.iFub;

import org.apache.giraph.aggregators.LongMaxAggregator;
import org.apache.giraph.aggregators.LongSumAggregator;
import org.apache.giraph.examples.SimpleAggregatorWriter;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.LongWritable;
import org.apache.log4j.Logger;
/*
 * This class is used to manage the computation of iFUB algorithm
 * It's a bit messy but it works !
 * Severals aggregator are used
 */
public class iFubMasterCompute extends DefaultMasterCompute {
	public static String LOWER_B = "Lowerbound";
	public static String LAYER = "Layer";
	public static String SOURCE_ID = "SourceId";
	public static String MAX_DEGREE = "MaxDegree";
	public static String NUM_ACTIVE_VERTEX = "ActiveVertex";
	public static String BFS_STEP = "BFSStep";
	
	private static long bfsCounter;
	
	private Logger LOG = Logger.getLogger(iFubMasterCompute.class);
	
	
	@Override
	public void compute() {
		printAggregators();
		// select the root vertex for the entire algorithm
		
		if (getSuperstep() == 0) {
			if (LOG.isInfoEnabled()) LOG.info("Computing max degree");
			setComputation(MaxDegreeComputation.class);
		}
		
		else if (getSuperstep() == 1)
		{
			if (LOG.isInfoEnabled()) LOG.info("Selecting source vertex for 2 sweep");
		}
		else
		{
			// if we tried to select a vertex at the previous step, we start a new BFS
			if (getComputation() == iFubSelectSource.class) {
				//on vérifie qu'on a bien trouvé une source pour la couche donnée
				//we verify that a source have been found, not an error case, it's just mean the layer have been entirely explored 
				if (((LongWritable)getAggregatedValue(SOURCE_ID)).get() == -1) {
					if (LOG.isInfoEnabled()) {
						LOG.info("No vertex found for layer : " + ((LongWritable)getAggregatedValue(LAYER)).get());
					}
					//we check the halting condition
					this.checkHaltingAfterLayer();
					
					//on passe à la couche inf
					//go to next layer
					long previousLayer = ((LongWritable)getAggregatedValue(LAYER)).get();
					setAggregatedValue(LAYER, new LongWritable(previousLayer-1));
					if(previousLayer<0)
					{
						LOG.info("############################################################# ERROR !");
						haltComputation();
					}
				}
				else
				{
					//if we found a source, explore it.
					//BFS from the new source
					if (LOG.isInfoEnabled()) {
						LOG.info("New BFS, source vertex : " + ((LongWritable)getAggregatedValue(SOURCE_ID)).get());
					}
					newBFS();
				}
			}
			else if(getComputation() == iFubAssignLayer.class)
			{
				//If we just assigned the layers, it's time to select a source and start the exploration
				selectSource();
			}
			else if((getComputation() == MaxDegreeComputation.class))
			{
				//after getting the vertex with the highest degree, we do a 2-sweep = 2BFS
				newBFS();
			}

			//if the previous BFS is finished, we select a new source
			else if (((LongWritable)getAggregatedValue(NUM_ACTIVE_VERTEX)).get() == 0) {
				if(getComputation()==iFubBFSCompute.class)
				{
					//if 2-sweep isn't done, proceed
					if(bfsCounter<3)
					{
						setAggregatedValue(LAYER, getAggregatedValue(LOWER_B));
						if (bfsCounter==2)
						{
							//select a source from the center layer
							long centerLayer = ((LongWritable)getAggregatedValue(LOWER_B)).get()/2;
							
							setAggregatedValue(LAYER, new LongWritable( centerLayer));
						}
						setComputation(iFubAssignLayer.class);
					}
					else
					{
						checkHaltingAfterBFS();
						selectSource();
					}
				}
			}
			else if (((LongWritable)getAggregatedValue(NUM_ACTIVE_VERTEX)).get() > 0)
			{
				//continue BFS, reset #active vertices and increment BFS_STEP
				nextStepBFS();
			}

		}
	}
	private void selectSource()
	{
		setAggregatedValue(SOURCE_ID, new LongWritable(-1));
		setComputation(iFubSelectSource.class);
	}
	
	private void newBFS()
	{
		setAggregatedValue(BFS_STEP, new LongWritable(0));
		iFubMasterCompute.bfsCounter++;
		setComputation(iFubBFSCompute.class);
	}
	
	private void nextStepBFS()
	{
		setAggregatedValue(NUM_ACTIVE_VERTEX, new LongWritable(0));
		setAggregatedValue(BFS_STEP, new LongWritable(((LongWritable)getAggregatedValue(BFS_STEP)).get()+1));

	}
	
	private void printAggregators()
	{
		if(LOG.isInfoEnabled())
		{
			LOG.info("printAggregators SUperStep: "+getSuperstep());

			String value = getAggregatedValue(NUM_ACTIVE_VERTEX).toString();
			LOG.info("NUM_ACTIVE_VERTEX: "+value);
			value = getAggregatedValue(LOWER_B).toString();
			LOG.info("LOWER_B: "+value);

			value = getAggregatedValue(LAYER).toString();
			LOG.info("LAYER: "+value);
			value = getAggregatedValue(SOURCE_ID).toString();
			LOG.info("SOURCE_ID: "+value);
			value = getAggregatedValue(MAX_DEGREE).toString();
			LOG.info("MAX_DEGREE: "+value);
			value = getAggregatedValue(BFS_STEP).toString();
			LOG.info("BFS_STEP: "+value);

			LOG.info("BFS_COUNT: "+bfsCounter);
			LOG.info("computation class: "+getComputation().toString());

		}
	}
	/*
	 * Check if the halting condition is met after each bfs
	 * which is maxExentricity==currentLayer*2
	 * if not update the new Lower bound
	 */
	private void checkHaltingAfterBFS() {
		long lowerB = ((LongWritable)getAggregatedValue(LOWER_B)).get();
		long currentLayer = ((LongWritable)getAggregatedValue(LAYER)).get();
		
		long upperB = currentLayer*2;
		if(lowerB==upperB)
		{
			printResult(lowerB);
		}

	}
	private void printResult(long diameter)
	{
		System.out.println("Diameter ="+diameter);
		LOG.info("Diameter ="+diameter);
		
		getContext().getCounter("iFUB stats", "Diameter")
		.increment(diameter);
		getContext().getCounter("iFUB stats", "BFS")
		.increment(bfsCounter);
		
		this.haltComputation();
	}
	/*
	 * Check if the halting condition is met after exploring all the current layer
	 * which is lower bound > (currentLayer-1)*2
	 */
	private void checkHaltingAfterLayer() {
		// TODO Auto-generated method stub
		long lowerB = ((LongWritable)getAggregatedValue(LOWER_B)).get();
		long currentLayer = ((LongWritable)getAggregatedValue(LAYER)).get();
		if(lowerB>((currentLayer-1)*2))
		{
			printResult(lowerB);
		}
	}
	/*
	 * register the aggregators used in the algorithm
	 * and set the default values
	 */
	@Override
	public void initialize() throws InstantiationException, IllegalAccessException {
		
		registerPersistentAggregator(LOWER_B, LongMaxAggregator.class);
		registerPersistentAggregator(LAYER, LongMaxAggregator.class);
		registerPersistentAggregator(SOURCE_ID, LongMaxAggregator.class);
		registerPersistentAggregator(MAX_DEGREE, LongMaxAggregator.class);
		registerPersistentAggregator(NUM_ACTIVE_VERTEX, LongSumAggregator.class);
		registerPersistentAggregator(BFS_STEP, LongSumAggregator.class);
		

		
		setAggregatedValue(NUM_ACTIVE_VERTEX, new LongWritable(1));
		setAggregatedValue(LOWER_B, new LongWritable(-1));
		setAggregatedValue(LAYER, new LongWritable(-1));
		setAggregatedValue(SOURCE_ID, new LongWritable(-1));
		setAggregatedValue(MAX_DEGREE, new LongWritable(-1));
		setAggregatedValue(NUM_ACTIVE_VERTEX, new LongWritable(0));



	}

}
