import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class MyHBaseDAO {

	public static void insertRecord(HTableInterface table, HBaseTestObj obj)
			throws Exception {
		Put put = createPut(obj);
		table.put(put);
	}

	public static Put createPut(HBaseTestObj obj) {
		Put put = new Put(Bytes.toBytes(obj.getRowKey()));
		put.add(Bytes.toBytes("CF"), Bytes.toBytes("CQ-1"),
				Bytes.toBytes(obj.getData1()));
		put.add(Bytes.toBytes("CF"), Bytes.toBytes("CQ-2"),
				Bytes.toBytes(obj.getData2()));
		return put;
	}
}
