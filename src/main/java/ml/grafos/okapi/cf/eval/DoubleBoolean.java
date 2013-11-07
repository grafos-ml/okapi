package ml.grafos.okapi.cf.eval;

public class DoubleBoolean implements Comparable<DoubleBoolean>{
	private double score;
	private boolean relevant;
	
	public DoubleBoolean(double score, boolean relevant) {
		this.setScore(score);
		this.setRelevant(relevant);
	}
	
	/**
	 * For sorting from biggest to smallest score!
	 * Kind of inverse comparator.
	 */
	public int compareTo(DoubleBoolean o) {
		if (getScore() == o.getScore()){
			return 0;
		}else if (getScore() < o.getScore()){
			return 1;
		}else return -1;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public boolean isRelevant() {
		return relevant;
	}

	public void setRelevant(boolean relevant) {
		this.relevant = relevant;
	}
}
