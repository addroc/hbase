package hbase2hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//将hBase数据加载到hdfs

public class HBaseToHdfs {
	
	
	private static String tableName="ghgj";
	public static class HdfsMapper extends TableMapper<Text,NullWritable>{
		@Override
		protected void map(ImmutableBytesWritable key, Result value,
				Mapper<ImmutableBytesWritable, Result, Text,NullWritable>.Context context)
				throws IOException, InterruptedException {
			//将hbase传入的key转换成String的形式
			System.out.println("haha");
			byte[] copyBytes = key.copyBytes();
			String keyString = new String(copyBytes);
			
			byte[] value2 = value.getValue("student".getBytes(), "name".getBytes());
			String nameString = Bytes.toString(value2);
			context.write(new Text(keyString+"\t"+nameString),NullWritable.get());
			
		}
	}
	
	public static class HdfsReducer extends Reducer<Text,NullWritable, Text,NullWritable>{
		@Override
		protected void reduce(Text key, Iterable<NullWritable> value,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		String zk_list = "hadoop02:2181,hadoop03:2181,hadoop04:2181,hadoop05:2181";
		Configuration conf = HBaseConfiguration.create();
		//设置操作的用户
		System.setProperty("HADOOP_USER_NAME","hadoop");
		conf.set("hbase.zookeeper.quorum", zk_list);
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(HBaseToHdfs.class);
		Scan scan = new Scan();
		
		TableMapReduceUtil.initTableMapperJob(tableName, scan, HdfsMapper.class, Text.class, NullWritable.class, job);
		job.setReducerClass(HdfsReducer.class);
		
		Path outPath=new Path("D:\\maven\\maven_test\\outputfile");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath, true);
		}
		FileOutputFormat.setOutputPath(job, outPath);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		boolean waitForCompletion = job.waitForCompletion(true);
		System.exit(waitForCompletion ? 0:1);
	}
}
