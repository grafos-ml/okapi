package ml.grafos.okapi.cf.eval;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DoubleArrayListWritableTest {

	DoubleArrayListWritable a, b;
	
	@Before
	public void setUp() throws Exception {
		a = new DoubleArrayListWritable();
		b = new DoubleArrayListWritable();
		
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testAdd() {
		assertEquals(0, a.size());
		a.add(0.55);
		assertEquals(0.55, a.get(0).get(), 0.0001);
	}
	
	@Test
	public void testEquals(){
		assertTrue(a.equals(a));
		a.add(0.05);
		assertFalse(a.equals(b));
		b.add(0.05);
		assertTrue(a.equals(b));
	}

	@Test
	public void testSum(){
		assertEquals(0, a.sum(b).size());
		a.add(0.5);
		b.add(0.5);
		assertEquals(1, a.sum(b).size());
		assertEquals(1, a.sum(b).get(0).get(), 0.0001);
	}

	@Test(expected=IndexOutOfBoundsException.class)
	public void testSumThrows(){
		a.add(0.5);
		a.sum(b);
	}
	
	@Test
	public void testDot(){
		a.add(0.5);
		a.add(0.4);
		b.add(0.1);
		b.add(0.1);
		assertEquals(0.09, a.dot(b), 0.0001);
		assertEquals(0.25+0.16, a.dot(a), 0.001);
	}
	
	@Test(expected=IndexOutOfBoundsException.class)
	public void testDotThrows(){
		a.add(0.5);
		a.dot(b);
	}
	
	@Test
	public void testMul(){
		a.add(0.5);
		a.add(0.4);
		assertEquals(0.05, a.mul(0.1).get(0).get(), 0.0001);
		assertEquals(-0.04, a.mul(-0.1).get(1).get(), 0.0001);
	}
}
