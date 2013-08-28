import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

public class MyReducerTest {
	ReduceDriver<Text, Text, ImmutableBytesWritable, Writable> reduceDriver;
	byte[] CF = "CF".getBytes();
	byte[] QUALIFIER = "CQ-1".getBytes();

	@Before
	public void setUp() {
		MyReducer reducer = new MyReducer();
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	@Test
	public void testHBaseInsert() throws IOException {

		String strKey = "RowKey-1";
		String strValue = "DATA";
		String strValue1 = "DATA1";
		String strValue2 = "DATA2";

		List<Text> list = new ArrayList<Text>();
		list.add(new Text(strValue));
		list.add(new Text(strValue1));
		list.add(new Text(strValue2));

		// since in our case all that reducer is doing is appending all the
		// records that the mapper sends it, we should get the following back
		String expectedOutput = strValue + strValue1 + strValue2;

		// Setup Input, mimic what mapper would have passed to the reducer and
		// run test
		reduceDriver.withInput(new Text(strKey), list);
		// run the reducer and get its output
		List<Pair<ImmutableBytesWritable, Writable>> result = reduceDriver
				.run();

		// extract key from result and verify
		assertEquals(Bytes.toString(result.get(0).getFirst().get()), strKey);

		// extract value for CF/QUALIFIER and verify
		Put a = (Put) result.get(0).getSecond();
		String c = Bytes.toString(a.get(CF, QUALIFIER).get(0).getValue());
		assertEquals(expectedOutput, c);
	}

}