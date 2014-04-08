package ml.grafos.okapi.multistage.voting;

import com.google.common.collect.Multiset;
import ml.grafos.okapi.multistage.MultistageMasterCompute;
import ml.grafos.okapi.multistage.Stage;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

/**
 * Test for exclusive election vote.
 */
public class ExclusiveElectionTest {

  private ExclusiveElection instance;
  private MultistageMasterCompute master;
  Multiset<Integer> votes;

  @Before
  public void initialize() {
    instance = new ExclusiveElection();

    // Setup the mocking infrastructure to allow for the correct
    // vote list to be fetched from within the voting election
    IntMultisetWrapperWritable wrapper = new IntMultisetWrapperWritable();
    votes = wrapper.get();
    master = mock(MultistageMasterCompute.class);
  }

  @Test
  public void testResolveElectionNoVotes() {
    instance.resolveElection(master, votes);
    verify(master, never()).setStage(any(Stage.class));
    verify(master, never()).setStage(anyInt());
  }

  @Test
  public void testResolveElectionSingleVote() {
    votes.add(5);
    instance.resolveElection(master, votes);
    verify(master).setStage(5);
  }

  @Test(expected = IllegalStateException.class)
  public void testResolveElectionMultipleVotes() {
    votes.add(5);
    votes.add(5);
    instance.resolveElection(master, votes);
  }

  @Test(expected = IllegalStateException.class)
  public void testResolveElectionConflictingVotes() {
    votes.add(1);
    votes.add(2);
    instance.resolveElection(master, votes);
  }

}
