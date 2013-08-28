import static org.junit.Assert.*;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class MyHBaseIntegrationTest {
	private static HBaseTestingUtility utility;
	byte[] CF = "CF".getBytes();
	byte[] CQ1 = "CQ-1".getBytes();
	byte[] CQ2 = "CQ-2".getBytes();
	
	@Before
	public void setup() throws Exception {
		utility = new HBaseTestingUtility();
		utility.startMiniCluster();
	}

	@Test
	public void testInsert() throws Exception {
		HTableInterface table = utility.createTable(Bytes.toBytes("MyTest"),
				Bytes.toBytes("CF"));
		HBaseTestObj obj = new HBaseTestObj();
		obj.setRowKey("ROWKEY-1");
		obj.setData1("DATA-1");
		obj.setData2("DATA-2");
		MyHBaseDAO.insertRecord(table, obj);
		Get get = new Get(Bytes.toBytes(obj.getRowKey()));
		get.addColumn(CF, CQ1);
		Result result = table.get(get);
		assertEquals(Bytes.toString(result.getRow()), obj.getRowKey());
		assertEquals(Bytes.toString(result.value()), obj.getData1());
	}
}
