import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

public class MyReducer extends TableReducer<Text, Text, ImmutableBytesWritable> {
	public static final byte[] CF = "CF".getBytes();
	public static final byte[] QUALIFIER = "CQ-1".getBytes();

	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// bunch of processing to extract data to be inserted
		// in our case, lets say we are simply appending all the records we
		// received from the mapper..

		// create a put
		StringBuffer data = new StringBuffer();
		Put put = new Put(Bytes.toBytes(key.toString()));
		for (Text val : values) {
			data = data.append(val);
		}
		put.add(CF, QUALIFIER, Bytes.toBytes(data.toString()));

		// write to HBase
		context.write(
				new ImmutableBytesWritable(Bytes.toBytes(key.toString())), put);
	}
}
