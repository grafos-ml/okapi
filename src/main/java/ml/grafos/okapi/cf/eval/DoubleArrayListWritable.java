package ml.grafos.okapi.cf.eval;

import org.apache.giraph.utils.ArrayListWritable;
import org.apache.hadoop.io.DoubleWritable;

public class DoubleArrayListWritable extends ArrayListWritable<DoubleWritable>{
	private static final long serialVersionUID = 398739235208671119L;

	@Override
	public void setClass() {
		setClass(DoubleWritable.class);
	}
	
	public static DoubleArrayListWritable zeros(int size){
		DoubleArrayListWritable ret = new DoubleArrayListWritable();
		for(int i=0; i<size; i++){
			ret.add(0.0);
		}
		return ret;
	}
	
	public DoubleArrayListWritable() {
		super();
	}
	
	public void add(double d){
		super.add(new DoubleWritable(d));
	}

	@Override
	public boolean equals(Object o) {
		if (null == o || !(o instanceof DoubleArrayListWritable))
			return false;
		DoubleArrayListWritable other = (DoubleArrayListWritable)o;
		if (other.size() != this.size())
			return false;
		int size2 = this.size();
		for (int i = 0; i < size2; i++) {
			if (!(this.get(i).equals(other.get(i)))){
				return false;
			}
		}
		return true;
	}
	
	public DoubleArrayListWritable sum(DoubleArrayListWritable other){
		DoubleArrayListWritable ret = new DoubleArrayListWritable();
		int size = this.size();
		for(int i=0; i<size; i++){
			ret.add(new DoubleWritable(this.get(i).get() + other.get(i).get()));
		}
		return ret;
	}
	
	public DoubleArrayListWritable diff(DoubleArrayListWritable other){
		DoubleArrayListWritable ret = new DoubleArrayListWritable();
		int size = this.size();
		for(int i=0; i<size; i++){
			ret.add(new DoubleWritable(this.get(i).get() - other.get(i).get()));
		}
		return ret;
	}
	
	public DoubleArrayListWritable mul(double d){
		DoubleArrayListWritable ret = new DoubleArrayListWritable();
		int size = this.size();
		for(int i=0; i<size; i++){
			ret.add(new DoubleWritable(this.get(i).get()*d));
		}
		return ret;
	}
	
	public double dot(DoubleArrayListWritable v){
		double ret = 0;
		int size = this.size();
		for(int i=0; i<size; i++){
			ret += v.get(i).get() * this.get(i).get();
		}
		return ret;
	}
}
