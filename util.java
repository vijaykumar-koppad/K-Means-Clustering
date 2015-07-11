import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

/* This file has the helper functions which are used in mapreduce program.
 *
 * */
public class util {


	// helper function to create tables.

	public static void createTables(Configuration conf, String IpTable, String OpTable) throws MasterNotRunningException,
									ZooKeeperConnectionException, IOException{
		HBaseAdmin admin = new HBaseAdmin(conf);
		if(admin.tableExists(IpTable)){
			System.out.println("Table "+IpTable+" already existing, hence deleting it ..");
			admin.disableTable(IpTable);
			admin.deleteTable(IpTable);
		}
		System.out.println("Creating the table "+IpTable);
		HTableDescriptor table = new HTableDescriptor(IpTable);
		table.addFamily(new HColumnDescriptor("Area"));
		table.addFamily(new HColumnDescriptor("Property"));
		admin.createTable(table);

		if(admin.tableExists(OpTable)){
			System.out.println("Table "+OpTable+" already existing, hence deleting it ..");
			admin.disableTable(OpTable);
			admin.deleteTable(OpTable);
		}
		System.out.println("Creating the table "+OpTable);
		HTableDescriptor tab = new HTableDescriptor(OpTable);
		tab.addFamily(new HColumnDescriptor("Area"));
		tab.addFamily(new HColumnDescriptor("Property"));

		admin.createTable(tab);
		admin.close();
			}

		// Function to initialise the center table. Since number of the K will be very less ,
		// it won't affect the performance
	public static void initCenterTable(Configuration conf, int k,String IpTable, String OpTable){
		System.out.println("Initialising Center Table ... ");
		try{
			HTable table = new HTable(conf, IpTable);
			for(int i=0;i<k;i++){
				String rowkey = "row"+i;
				Get get = new Get(rowkey.getBytes());
				Result rs = table.get(get);
				for(KeyValue kv : rs.raw()){
					HTable centerTab = new HTable(conf, OpTable);
					Put put = new Put(Bytes.toBytes("cent"+i));
					String family = new String(kv.getFamily());
					String qual = new String(kv.getQualifier());
					String val = new String(kv.getValue());
					put.add(Bytes.toBytes(family), Bytes.toBytes(qual),Bytes.toBytes(val));
					centerTab.put(put);
					centerTab.close();
				}
				table.close();
			}
			} catch (IOException e){
				e.printStackTrace();
		}

			 System.out.println("Initialised Center Table.");

		}


	/* Function to read data from the center table.
	 * */
	public static  Map<String, HashMap<String, Float>> ReadCenterTable(String Optable) throws IOException{

		Map<String, HashMap<String, Float>> center= new HashMap<String, HashMap<String, Float>>();

		Configuration con =  HBaseConfiguration.create();
		HTable tab = new HTable(con, Optable);
		Scan s = new Scan();
		ResultScanner ss = tab.getScanner(s);
		for(Result r: ss){
			HashMap<String, Float> tmp;
			for(KeyValue kv: r.raw()){
				String key = new String(kv.getRow());
				if(center.containsKey(key)){
					tmp = center.get(key);
					tmp.put(new String(kv.getQualifier()), Float.parseFloat(new String(kv.getValue())));
				}
				else{
					tmp = new HashMap<String, Float>();
					tmp.put(new String(kv.getQualifier()), Float.parseFloat(new String(kv.getValue())));
				}
				center.put(key, tmp);

			}

		}
		tab.close();
		return center;
	}

	// Computes the newcenter by averaging the values.
	public static Map<String, Double> computeNewCenter(Text key, Iterable<Text> values, String IpTable) throws IOException{
		final byte[] area = "Area".getBytes();
		final byte[] prop = "Property".getBytes();
		Map<String, Double> newCenter = new HashMap<String, Double>();
		Configuration config =  HBaseConfiguration.create();
		HTable table = new HTable(config, IpTable);
		double X1=0,X2=0,X3=0,X4=0,X5=0,X6=0,X7=0,X8=0,Y1=0,Y2=0;
		int cnt = 0;

		for (Text val : values) {
			Get get = new Get(val.toString().trim().getBytes());
			Result rs = table.get(get);
			X1 += Float.parseFloat(new String(rs.getValue(area, "X1".getBytes())));
			X5 += Float.parseFloat(new String(rs.getValue(area, "X5".getBytes())));
			X6 += Float.parseFloat(new String(rs.getValue(area, "X6".getBytes())));
			Y1 += Float.parseFloat(new String(rs.getValue(area, "Y1".getBytes())));
			Y2 += Float.parseFloat(new String(rs.getValue(area, "Y2".getBytes())));
			X2 += Float.parseFloat(new String(rs.getValue(prop, "X2".getBytes())));
			X3 += Float.parseFloat(new String(rs.getValue(prop, "X3".getBytes())));
			X4 += Float.parseFloat(new String(rs.getValue(prop, "X4".getBytes())));
			X7 += Float.parseFloat(new String(rs.getValue(prop, "X7".getBytes())));
			X8 += Float.parseFloat(new String(rs.getValue(prop, "X8".getBytes())));
			cnt++;
		}

		newCenter.put("X1", X1/cnt);newCenter.put("X2", X2/cnt);
		newCenter.put("X3", X3/cnt);newCenter.put("X4", X4/cnt);
		newCenter.put("X5", X5/cnt);newCenter.put("X6", X6/cnt);
		newCenter.put("X7", X7/cnt);newCenter.put("X8", X8/cnt);
		newCenter.put("Y1", Y1/cnt);newCenter.put("Y2", Y2/cnt);

		System.out.println(newCenter.toString());
		table.close();

		return newCenter;

	}

	// Computes the distance between new center calculated and old center in the center table.
	// Compares it with 0.1
	public static boolean computeDistance(Text key, Map<String,Double> center, String Optable) throws IOException{

		double sum = 0;
		Configuration con =  HBaseConfiguration.create();
		HTable tab = new HTable(con, Optable);
		Get get = new Get(key.getBytes());
		Result rs = tab.get(get);
		for(KeyValue kv: rs.raw()){
			double u =  center.get(new String(kv.getQualifier()));
			double x = Float.parseFloat(new String(kv.getValue()));
			sum += Math.pow((x-u), 2);
		}

		return (sum <= 0.1);

	}

}
