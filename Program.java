import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ServiceException;


public class Program extends Configured implements Tool {

	public enum STATE{
		Counter;
	}

	public static class WriteMapper extends Mapper<LongWritable, Text, IntWritable, Text>  {

			IntWritable k = new IntWritable();
			Text res = new Text();
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

				String val = value.toString();
				int row = val.hashCode();
				k.set(row);
				res.set(val);
				context.write(k, res);
		    }

	}

	public static class WriteReducer extends TableReducer<IntWritable, Text, Text>  {
		  public static final byte[] area = "Area".getBytes();
		  public static final byte[] prop = "Property".getBytes();
		  private Text rowkey = new Text();
		  private int rowCount = 0;
		  public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String X1 ="",X2="",X3="",X4="",X5="",
					X6="",X7="",X8="",Y1="",Y2="";
		    for (Text val : values) {
		      String[] v = val.toString().split("\t");
		      X1 = v[0]; X2 = v[1]; X3 = v[2];
		      X5 = v[4]; X4 = v[3]; X6 = v[5];
		      X7 = v[6]; X8 = v[7]; Y1 = v[8];
		      Y2 = v[9];
		    }
		    String k = "row"+rowCount;
		    Put put = new Put(Bytes.toBytes(k.toString()));
		    put.add(area, "X1".getBytes(), Bytes.toBytes(X1));
		    put.add(area, "X5".getBytes(), Bytes.toBytes(X5));
		    put.add(area, "X6".getBytes(), Bytes.toBytes(X6));
		    put.add(area, "Y1".getBytes(), Bytes.toBytes(Y1));
		    put.add(area, "Y2".getBytes(), Bytes.toBytes(Y2));
		    put.add(prop, "X2".getBytes(), Bytes.toBytes(X2));
		    put.add(prop, "X3".getBytes(), Bytes.toBytes(X3));
		    put.add(prop, "X4".getBytes(), Bytes.toBytes(X4));
		    put.add(prop, "X7".getBytes(), Bytes.toBytes(X7));
		    put.add(prop, "X8".getBytes(), Bytes.toBytes(X8));
		    rowCount++;
		    rowkey.set(k);
		    context.write(rowkey, put);
		  }


	}


	public static class KmeansMapper extends TableMapper<Text, Text>  {

		  private Text key  = new Text();
		  private Text val = new Text();
		  private Map<String, HashMap<String, Float>> center= new HashMap<String, HashMap<String, Float>>();

		  @Override
		  public void setup(Context context) throws IOException,
					InterruptedException{
			  Configuration conf = context.getConfiguration();
			  try {
				HBaseAdmin.checkHBaseAvailable(conf);
			} catch (ServiceException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			  String OpTable = conf.get("OpTable");

				center = util.ReadCenterTable(OpTable);
				super.setup(context);
		  }

		  public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException, InterruptedException {

			  Double min = Double.POSITIVE_INFINITY;

			  Configuration conf = context.getConfiguration();
			  int K = Integer.parseInt(conf.get("K"));

			  for(int i=0; i<K; i++){
				  double sum = 0;
				  String cen = "cent"+i;
				  HashMap<String, Float> rowValues = center.get(cen);
				  for(KeyValue kv: value.raw()){
					  float u =  rowValues.get(new String(kv.getQualifier()));
					  float x = Float.parseFloat(new String(kv.getValue()));
					  sum += Math.pow((x-u), 2);
				  }
				  if(sum < min){
					  min = sum;
					  key.set(cen);
				  }

			  }
			  val.set(Bytes.toString(row.get()));
		      context.write(key, val);
		    }

	}

	public static class KmeansReducer extends TableReducer<Text, Text, Text>  {
		 public static final byte[] area = "Area".getBytes();
		 public static final byte[] prop = "Property".getBytes();
		 public List<Boolean> converged = new ArrayList<Boolean>();

		 public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

				Map<String, Double> newCenter;

				Configuration conf = context.getConfiguration();
				int K = Integer.parseInt(conf.get("K"));
				String IpTable = conf.get("IpTable");
				String OpTable = conf.get("OpTable");

				newCenter = util.computeNewCenter(key, values, IpTable);

				converged.add(util.computeDistance(key, newCenter, OpTable));

			    Put put = new Put(Bytes.toBytes(key.toString()));
			    put.add(area, "X1".getBytes(), Bytes.toBytes(""+newCenter.get("X1")));
			    put.add(area, "X5".getBytes(), Bytes.toBytes(""+newCenter.get("X5")));
			    put.add(area, "X6".getBytes(), Bytes.toBytes(""+newCenter.get("X6")));
			    put.add(area, "Y1".getBytes(), Bytes.toBytes(""+newCenter.get("Y1")));
			    put.add(area, "Y2".getBytes(), Bytes.toBytes(""+newCenter.get("Y2")));
			    put.add(prop, "X2".getBytes(), Bytes.toBytes(""+newCenter.get("X2")));
			    put.add(prop, "X3".getBytes(), Bytes.toBytes(""+newCenter.get("X3")));
			    put.add(prop, "X4".getBytes(), Bytes.toBytes(""+newCenter.get("X4")));
			    put.add(prop, "X7".getBytes(), Bytes.toBytes(""+newCenter.get("X7")));
			    put.add(prop, "X8".getBytes(), Bytes.toBytes(""+newCenter.get("X8")));

			context.write(key, put);


		}

		@Override
		protected void cleanup(Context context) throws IOException,
												InterruptedException {
			for(boolean val: converged){
				boolean v = val;
				System.out.println(v);
				if(v == false){
					context.getCounter(STATE.Counter).increment(1);
				}
			}

		}
	}


	public static void main(String[] args) throws Exception {
			String IpTable = "Energy";
			String OpTable = "center";
			Configuration conf =  HBaseConfiguration.create();

			util.createTables(conf, IpTable, OpTable);

			int res = ToolRunner.run(conf, new Program(), args);
		System.exit(res);

	}

	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
			String IpTable = "Energy";
			String OpTable = "center";

			// First mapreduce stage.
			Configuration conf = getConf();
			String inputPath = args[0];
			int K = Integer.parseInt(args[1]);
			conf.set("K", ""+K);
			Job job = new Job(conf,"HBase_write");
			job.setInputFormatClass(TextInputFormat.class);
			job.setJarByClass(Program.class);
			job.setMapperClass(Program.WriteMapper.class);
			job.setReducerClass(Program.WriteReducer.class);
			job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);


		TableMapReduceUtil.initTableReducerJob(
				IpTable,        // output table
				WriteReducer.class,    // reducer class
				job);

		/* Setting the number of reducer to 1, to that we can assign the
		 * rowkey as row1, row2 ... */
		job.setNumReduceTasks(1);

		FileInputFormat.setInputPaths(job, inputPath);
		job.waitForCompletion(true);

		////////////////////////////////////////////////////////////////////

		util.initCenterTable(conf, K, IpTable, OpTable); // Initialising the center table

		////////////////////////////////////////////////////////////////////

		int count = 100; // Setting the higher limit,  in case of non-convergent dataset.

		boolean res= false;

		// Start of iterative map-reduce. This counter is just for maximum number iterations.
		// For this, it is enough. For other example it can be changed.  Its just a  safety net.

		System.out.println("Starting iterative mapreduce");
		while(count > 0){

			Configuration conf1 = HBaseConfiguration.create();

			conf1.set("K", ""+K);
			conf1.set("IpTable", IpTable);
			conf1.set("OpTable", OpTable);

			Job job1 = new Job(conf1,"k-map");

			job1.setJarByClass(Program.class);

			Scan scan = new Scan();
			scan.setCaching(500);
			scan.setCacheBlocks(false);
			TableMapReduceUtil.initTableMapperJob(
					IpTable,      // input table
					scan,             // Scan instance to control CF and attribute selection
					KmeansMapper.class,   // mapper class
					Text.class,             // mapper output key
					Text.class,             // mapper output value
					job1);
			TableMapReduceUtil.initTableReducerJob(
					OpTable,      // output table
					KmeansReducer.class,             // reducer class
					job1);


			res = job1.waitForCompletion(true);


			count--;

			// Termination condition is checked.
			if(job1.getCounters().findCounter(STATE.Counter).getValue() == 0)
				break;
		}

		return(res ? 0 : 1 );

	 }

}
