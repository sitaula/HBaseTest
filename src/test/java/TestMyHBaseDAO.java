import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class TestMyHBaseDAO {
	@Mock  
	 private HTableInterface table;
	@Mock
	private HTablePool hTablePool;
	@Captor
	private ArgumentCaptor<Put> putCaptor;

	@Test
	public void testInsertRecord() throws Exception {
		// return mock table when getTable is called
		when(hTablePool.getTable("tablename")).thenReturn(table);
		// make a call to above DAO
		HBaseTestObj obj = new HBaseTestObj();
		obj.setRowKey("ROWKEY-1");
		obj.setData1("DATA-1");
		obj.setData2("DATA-2");
		MyHBaseDAO.insertRecord(table, obj);
		verify(table).put(putCaptor.capture());
		Put put = putCaptor.getValue();

		assertEquals(Bytes.toString(put.getRow()), obj.getRowKey());
		assert (put.has(Bytes.toBytes("CF"), Bytes.toBytes("CQ-1")));
		assert (put.has(Bytes.toBytes("CF"), Bytes.toBytes("CQ-2")));
		assertEquals(
				Bytes.toString(put
						.get(Bytes.toBytes("CF"), Bytes.toBytes("CQ-1")).get(0)
						.getValue()), "DATA-1");
		assertEquals(
				Bytes.toString(put
						.get(Bytes.toBytes("CF"), Bytes.toBytes("CQ-2")).get(0)
						.getValue()), "DATA-2");
	}
	
	
}
