package ml.grafos.okapi.multistage.voting;

import com.google.common.collect.Multiset;
import ml.grafos.okapi.multistage.MultistageMasterCompute;
import ml.grafos.okapi.multistage.Stage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Test for exclusive election vote.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(MultistageMasterCompute.class)
public class MajorityElectionTest {

  private MajorityElection instance;
  private MultistageMasterCompute master;
  Multiset<Integer> votes;

  @Before
  public void initialize() {
    instance = new MajorityElection();

    // Setup the mocking infrastructure to allow for the correct
    // vote list to be fetched from within the voting election
    IntMultisetWrapperWritable wrapper = new IntMultisetWrapperWritable();
    votes = wrapper.get();
    master = mock(MultistageMasterCompute.class);
    when(master.getAggregatedValue(TransitionElection.AGGREGATOR_VOTING))
        .thenReturn(wrapper);
  }

  @Test
  public void testResolveElectionNoVotes() {
    instance.resolveElection(master);
    verify(master, never()).setStage(any(Stage.class));
    verify(master, never()).setStage(anyInt());
  }

  @Test
  public void testResolveElectionSingleVote() {
    votes.add(5);
    instance.resolveElection(master);
    verify(master).setStage(5);
  }

  @Test
  public void testResolveElectionMultipleEqualVotes() {
    votes.add(5);
    votes.add(5);
    instance.resolveElection(master);
    verify(master).setStage(5);
  }

  @Test
  public void testResolveElectionWithTies() {
    votes.add(1);
    votes.add(2);
    instance.resolveElection(master);
    verify(master, never()).setStage(any(Stage.class));
    verify(master, never()).setStage(anyInt());
  }

  @Test
  public void testResolveElectionMultipleDifferingVotes() {
    votes.add(5);
    votes.add(5);
    votes.add(2);
    instance.resolveElection(master);
    verify(master).setStage(5);
  }

}
