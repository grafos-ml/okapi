package ml.grafos.okapi.kmeans;

import java.util.Random;
import org.apache.giraph.aggregators.Aggregator;
import org.apache.giraph.conf.DefaultImmutableClassesGiraphConfigurable;

@SuppressWarnings("rawtypes")
public class ArrayListOfDoubleArrayListWritableAggregator extends 
	DefaultImmutableClassesGiraphConfigurable 
	implements Aggregator<ArrayListOfDoubleArrayListWritable> {
	
	public static final String CLUSTER_CENTERS_COUNT = "kmeans.cluster.centers.count";
    public static final String POINTS_COUNT = "kmeans.points.count"; 

	private int k; // the number of the cluster centers
	private int pointsCount; // the number of input points
	private ArrayListOfDoubleArrayListWritable value;
	
	public ArrayListOfDoubleArrayListWritableAggregator() {
		setAggregatedValue(createInitialValue());
	}
	
	/**
	 * Used to randomly select initial points for k-means
	 * If the size of the current list is less than k (#centers)
	 * then the element is appended in the list
	 * else it replaces an element in a random position
	 * with probability k/N, where N is the total number of points
	 * 
	 * @param other
	 */
	@Override
	public void aggregate(ArrayListOfDoubleArrayListWritable other) {
		k = getConf().getInt(CLUSTER_CENTERS_COUNT, 0);
		pointsCount = getConf().getInt(POINTS_COUNT, 0);
		for ( int i = 0;  i < other.size(); i++ ) {
			if ( getAggregatedValue().size() < k ) {
				value.add(other.get(i)); 
			}
			else  {
				Random ran = new Random();
				int index = ran.nextInt(k);
				if (Math.random() > ((double) k / (double) pointsCount) ) {
					value.set(index, other.get(i));
				}
			}
		}
		
	}

	@Override
	public ArrayListOfDoubleArrayListWritable createInitialValue() {
		return new ArrayListOfDoubleArrayListWritable();
	}

	@Override
	public ArrayListOfDoubleArrayListWritable getAggregatedValue() {
		return value;
	}

	@Override
	public void setAggregatedValue(ArrayListOfDoubleArrayListWritable value) {
		this.value = value;		
	}

	@Override
	public void reset() {
		value = createInitialValue();		
	}

}
