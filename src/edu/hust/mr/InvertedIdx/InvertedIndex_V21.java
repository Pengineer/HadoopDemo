package edu.hust.mr.InvertedIdx;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;


/**
 * InvertedIndex.class输出结果如下：
 * Everything	1.txt:1;
 * MapReduce	1.txt:1;2.txt:1;3.txt:1;
 * hello	3.txt:1;
 * is	2.txt:1;1.txt:2;
 * powerful	2.txt:1;
 * simple	2.txt:1;1.txt:2;
 * 
 * 这并不是实际需要的，我们需要将权值大的放在最前面，也就是说value要按照权值排序。
 * 
 * 如果数据量很大，我们是不能在本地直接将所有value值放到内存，然后进行排序。其实，可以借助MapReduce自带的对key的排序功能来实现这个需求，
 * 那么就需要将待排序的部分从value中移到key中，形成复合键。
 * 但是这有一个问题就是，我们不能保证相同的单词交给同一个reduce来处理，因为它们在不同文件中的词频不一定相同，这样输出的结果就不一定正确。
 * 那么我们就得自定义这个分区方式，不能直接根据key要分区，而是只取其中的单词来分区。
 * 
 * 思路1：(w-单词，f-词频，d-文件)
 * Map端：
 * 1，map()：将每个文件按照<w, d>键值对的形式输出；
 * 2，combiner()：将每个文件中相同的单词累加，统计出该文件的单词词频，形成新的键值对<w:f, d>；
 * 3，MapReduce框架会自动将combiner的输出结果按key进行多次排序，<w1:2, d1> <w1:3, d3> <w2:2, d1> <w3:1, d2>....；
 * 4，patition()：将排序后的结果进行分区，使用自定义分区方式（第3步排序时为了方便第4步的快速分区）；
 * Reduce端：
 * 5，MapReduce框架自动将每个分区的结果进行排序汇总，形成<key,list<value>>；
 * 6，reduce()：生成文档列表，输出。
 * 
 */
public class InvertedIndex_V21 extends Configured implements Tool {
	
	enum counter {
		LINESKIP
	}
	
	static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
		Text outputKey = new Text();
		Text outputValue = new Text();
		FileSplit split;
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			try {
				split = (FileSplit) context.getInputSplit();
				StringTokenizer strs = new StringTokenizer(value.toString());
				while (strs.hasMoreElements()) {
					String str = (String) strs.nextElement();
					outputKey.set(str + ":1");
					outputValue.set(split.getPath().getName());
					context.write(outputKey, outputValue);
				}
			} catch (Exception e) {
				context.getCounter(counter.LINESKIP).increment(1);//源文件格式错误，则跳过，计数器+1
				return;
			}
		}
	}
	
	/**
	 * 统计各文档的词频
	 */
	static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
		Text outputKey = new Text();
		Text outputValue = new Text();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			try {
				//<word, list<doc1,doc1,doc1>> 统计word的在单文件中的词频
				int sum =0;
				for (Text value : values) {
					sum++;
				}
				
				String outputkey = key.toString() + ":" + sum;
				
				
				outputKey .set(outputkey);
				context.write(outputKey, values.iterator().next());
			} catch (Exception e) {
				context.getCounter(counter.LINESKIP).increment(1);//源文件格式错误，则跳过，计数器+1
				return;
			}
		}
	}
	
	/**
	 * 按单词分区
	 */
	static class WordPatitioner extends HashPartitioner<Text, Text> {
		public int getPartition(Text key, Text value, int numReduceTasks) {
			key = new Text(key.toString().split(":")[0]);
			return super.getPartition(key , value, numReduceTasks);
		};
	}
	
	/**
	 * 生成文档列表
	 */
	static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
//		每个分区的输入数据形式是：<w1:2, d1> <w1:3, d3>，这样在reduce端，相同的单词的value并不能形成一个list集合
//		可以将本次job交给下一个job来继续实现文档列表的生成
		
		//调整思路，将上面的combiner直接作为reduce
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println("Usage: InvertedIndex <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, InvertedIndex_V21.class.getSimpleName());
		
		job.setJarByClass(InvertedIndex_V21.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(InvertedIndexMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setCombinerClass(InvertedIndexCombiner.class);
		
		job.setReducerClass(InvertedIndexReducer.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		boolean isSuccess = job.waitForCompletion(true);
		
		return isSuccess ? 0 : 1;
	}
	
}
