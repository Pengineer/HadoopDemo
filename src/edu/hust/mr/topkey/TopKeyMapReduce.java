package edu.hust.mr.topkey;

import java.io.IOException;
import java.util.TreeSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * 统计并找出播放次数最多前三名的   歌曲名+歌手名+播放次数
 * 
 * 原始文件清洗后的格式：歌曲语种   歌曲名称  歌手名  收藏次数  播放次数  出版时间
 * 
 */
public class TopKeyMapReduce extends Configured implements Tool {
	
	private static final int TOPSIZE = 3;
	
	public static class TopKeyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		private Text outputKey = new Text();
		private LongWritable outputValue = new LongWritable();
		
		@Override
		public void setup(Context context) throws IOException,
		InterruptedException {
			super.setup(context);
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String lineValue = value.toString();
			if (null == lineValue){
				return;
			}
			String[] strs = lineValue.split("\t");
			
			outputKey.set(strs[1] + "\t" + strs[2]);
			outputValue.set(Long.parseLong(strs[4]));
			
			context.write(outputKey, outputValue);
		}

		@Override
		public void cleanup(Context context) throws IOException,
		InterruptedException {
			super.cleanup(context);
		}
	}
	
	//将map的key和value合并当作reduce的key输出，reduce的value不输出
	public static class TopKeyReducer extends Reducer<Text, LongWritable, TopKeyWritable, NullWritable> {
		
		//Tree集合中的元素是有序的，默认采用元素的compareTo方法升序排序
		TreeSet<TopKeyWritable> set = new TreeSet<TopKeyWritable>();
		
		@Override
		public void setup(Context context)
				throws IOException, InterruptedException {
			super.setup(context);
		}
		
		@Override
		public void reduce(Text key, Iterable<LongWritable> value, Context context)
				throws IOException, InterruptedException {
			if (null == key) {
				return;
			}
			Long sum = 0L;
			for(LongWritable val : value) {
				sum += val.get();
			}
			String line = key.toString();
			String[] strs = line.split("\t");
			String songName = strs[0];
			String singerName = strs[1];
			Long playTimes = sum;
			
			set.add(new TopKeyWritable(songName, singerName, playTimes));
			if (set.size() > TOPSIZE) {
				set.remove(set.last());
			}
		}

		//将满足条件的元素暂存在集合中，reduce结束后再输出
		@Override
		public void cleanup(Context context)
				throws IOException, InterruptedException {
			for (TopKeyWritable value : set) {
				context.write(value, NullWritable.get()); //空的输出
			}
		}
	}
	
	@Override
	public int run(String[] args) throws Exception{
		Configuration conf = new Configuration();
		
		Job job = new Job(conf, TopKeyMapReduce.class.getSimpleName());
		
		job.setJarByClass(TopKeyMapReduce.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		job.setMapperClass(TopKeyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setReducerClass(TopKeyReducer.class);
		job.setOutputKeyClass(TopKeyWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		Boolean isSuccess = job.waitForCompletion(true);
		
		return isSuccess ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		args = new String[]{
				"hdfs://master:9000/user/hadoop/JavaTest/singer.data",
				"hdfs://master:9000/user/hadoop/JavaTestOut"
		};
		
		int status = new TopKeyMapReduce().run(args);
		
		System.exit(status);
	}
}
