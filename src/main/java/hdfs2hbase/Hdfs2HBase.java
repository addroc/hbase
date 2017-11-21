package hdfs2hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class Hdfs2HBase {
	
	public static String tableName="ghgj_student";
	public static String familyName="base_info";
	public static class HBaseMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			context.write(value, NullWritable.get());
		}
	}
	
	
	public static class HBaseReducer extends TableReducer<Text, NullWritable,ImmutableBytesWritable>{
		@Override
		protected void reduce(Text key, Iterable<NullWritable> value,Context context)
				throws IOException, InterruptedException {
			
			String[] split = key.toString().split(",");
			Put put=new Put(split[0].getBytes());
			put.addColumn(familyName.getBytes(),"name".getBytes(), split[1].getBytes());
			put.addColumn(familyName.getBytes(),"age".getBytes(), split[2].getBytes());
			context.write(new ImmutableBytesWritable(),put);
			
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		String zk_list = "hadoop02:2181,hadoop03:2181,hadoop04:2181,hadoop05:2181";
		Configuration conf = HBaseConfiguration.create();
		//设置操作的用户
		System.setProperty("HADOOP_USER_NAME","hadoop");
		conf.set("hbase.zookeeper.quorum", zk_list);
		
		//创建表的程序
		Connection cc = ConnectionFactory.createConnection(conf);
		Admin admin = cc.getAdmin();
		boolean tableExists = admin.tableExists(TableName.valueOf(tableName.getBytes()));
		if(tableExists){
			admin.disableTable(TableName.valueOf(tableName.getBytes()));
			admin.deleteTable(TableName.valueOf(tableName.getBytes()));
		}
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName.getBytes()));
		htd.addFamily(new HColumnDescriptor(familyName.getBytes()));
		admin.createTable(htd);
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(Hdfs2HBase.class);
		job.setMapperClass(HBaseMapper.class);
		job.setReducerClass(HBaseReducer.class);
		TableMapReduceUtil.initTableReducerJob(tableName,HBaseReducer.class, job);
		FileInputFormat.setInputPaths(job, new Path("D:\\maven\\maven_test\\inputfiel"));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(ImmutableBytesWritable.class);
		job.setOutputValueClass(Mutation.class);
		boolean waitForCompletion = job.waitForCompletion(true);
		System.exit(waitForCompletion ? 0:1);
	}

}
