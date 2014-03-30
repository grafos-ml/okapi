/**
 * Copyright 2014 Grafos.ml
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
package ml.grafos.okapi.multistage.voting;

import com.google.common.collect.Multiset;
import com.google.common.collect.Ordering;
import ml.grafos.okapi.multistage.MultistageMasterCompute;
import org.apache.log4j.Logger;

import java.util.Comparator;
import java.util.List;

/**
 * Resolves the election using a majority vote (the stage transition with
 * the most vote wins). If there is a tie, no transition wins and the
 * current stage is maintained.
 */
public class MajorityElection extends AbstractTransitionElection {
  private static Logger logger = Logger.getLogger(MajorityElection.class);

  /**
   * Ordering employed to resolve voting elections.
   */
  private static final Ordering<Multiset.Entry<Integer>> ORDER = Ordering.from(
    new Comparator<Multiset.Entry<Integer>>() {
      @Override
      public int compare(Multiset.Entry<Integer> o1, Multiset.Entry<Integer> o2) {
        return Integer.compare(o1.getCount(), o2.getCount());
      }
    }
  );

  @Override
  protected void resolveElection(MultistageMasterCompute master, Multiset<Integer> votes) {
    List<Multiset.Entry<Integer>> best2 = ORDER.greatestOf(votes.entrySet(), 2);

    // If there is only one element, that one wins. Otherwise, make sure that there is
    // no tie between the best two elements.
    if (best2.size() < 2 || best2.get(0).getCount() > best2.get(1).getCount()) {
      master.setStage(best2.get(0).getElement());
    }
  }
}
