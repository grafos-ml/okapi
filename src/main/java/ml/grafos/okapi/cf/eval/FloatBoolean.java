/**
 * Copyright 2013 Grafos.ml
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
