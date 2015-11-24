package edu.hust.mr.db;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 为了方便MapReduce直接访问关系型数据库（Mysql,Oracle），Hadoop提供了DBInputFormat和DBOutputFormat两个类。
 * DBInputFormat类:把数据库表数据读入到HDFS；
 * DBOutputFormat类:把MapReduce产生的结果集导入到数据库表中；
 * DBConfiguration类:提供数据库配置和创建连接的接口，使用静态方法configureDB即可。
 * 
 * @author hadoop1.2.1
 *
 * status：failed
 * 不知道是不是Hadoop的该版本对Oracle的支持不是很好，网上很多MySQL的都可以实现
 */

public class DBDemo {
	
	public static class DBAccessMapper extends Mapper<LongWritable, TableRecordInfo, IntWritable, Text> {
		IntWritable outputKey = new IntWritable();
		Text outputValue = new Text();
		
		@Override
		public void map(LongWritable key, TableRecordInfo value, Context context) throws IOException, InterruptedException {
			outputKey.set(value.getId());
			outputValue.set(value.getName());
			context.write(outputKey, outputValue);
		}
	}
	
	public int writeRun(String outputPath) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, DBDemo.class.getSimpleName());
		job.setJarByClass(DBDemo.class);
		
		job.setMapperClass(DBAccessMapper.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(DBInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(outputPath));
		
		DBInputFormat.setInput(job, TableRecordInfo.class,"T_TEST", null, null, "ID", "NAME");
		DBConfiguration.configureDB(conf, 
									"oracle.jdbc.OracleDriver", 
									"jdbc:oracle:thin:@192.168.88.176:1521:orcl", 
									"scott", 
									"peng123");
		
		DBInputFormat dbInputFormat = new DBInputFormat();
		dbInputFormat.setConf(conf);
		conf.set(DBConfiguration.INPUT_TABLE_NAME_PROPERTY, "T_TEST");
		conf.setStrings(DBConfiguration.INPUT_FIELD_NAMES_PROPERTY, "ID","NAME");
		conf.set(DBConfiguration.DRIVER_CLASS_PROPERTY, "oracle.jdbc.OracleDriver");

		
		
		boolean isSuccess = job.waitForCompletion(true);
		return isSuccess ? 0 : 1;
	}
	
	
	public static void main(String[] args) throws Exception {
		args = new String[]{"hdfs://master:9000/user/hadoop/FromDB"};
		int status = new DBDemo().writeRun(args[0]);
		System.exit(status);
	}
}
