package ml.grafos.okapi.cf.eval;

public class FloatBoolean implements Comparable<FloatBoolean>{
	private float score;
	private boolean relevant;
	
	public FloatBoolean(float score, boolean relevant) {
		this.setScore(score);
		this.setRelevant(relevant);
	}
	
	/**
	 * For sorting from biggest to smallest score!
	 * Kind of inverse comparator.
	 */
	public int compareTo(FloatBoolean o) {
		if (getScore() == o.getScore()){
			return 0;
		}else if (getScore() < o.getScore()){
			return 1;
		}else return -1;
	}

	public float getScore() {
		return score;
	}

	public void setScore(float score) {
		this.score = score;
	}

	public boolean isRelevant() {
		return relevant;
	}

	public void setRelevant(boolean relevant) {
		this.relevant = relevant;
	}
}
