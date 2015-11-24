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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;


/**
 * 根据文档内容来确定文档，为每一个词建立一个索引（文档列表），还可以为文档添加权重，它与传统的根据文档来确定其包含的内容相反，因此称为倒排索引。
 * 此处只演示英文全文检索，中文分词较复杂，需要第三方API支持。
 * 
 * word1:document1(weight)-->document3(weignt)-->document8(weight)
 * word2:document3(weight)-->document5(weignt)-->document16(weight)
 * 
 * weight即表示权重，本设计中表示单词在文档中出现的次数。
 * 显然要有三个信息：单词，文档，词频，既要统计词频，又要生成文档列表，一个reduce只能完成一个，可以借助combiner完成词频统计。
 *
 */
public class InvertedIndex extends Configured implements Tool {
	
	enum counter {
		LINESKIP
	}
	
	/**
	 * <0, "MapReduce is simple">  -->  "MapReduce:0.txt" 1  \n  "is:0.txt" 1  \n "simple:0.txt" 1
	 */
	static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
		Text outputKey = new Text();
		Text outputValue = new Text();
		FileSplit split;
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			try {
				split = (FileSplit) context.getInputSplit();
				StringTokenizer strs = new StringTokenizer(value.toString());
				while (strs.hasMoreElements()) {
					String str = (String) strs.nextElement();
					outputKey.set(str + ":" + split.getPath().getName());
					outputValue.set("1");
					context.write(outputKey, outputValue);
				}
			} catch (Exception e) {
				context.getCounter(counter.LINESKIP).increment(1);//源文件格式错误，则跳过，计数器+1
				return;
			}
		}
	}
	
	/**
	 * 各文档的词频统计
	 * "MapReduce:0.txt" 1  \n  "is:0.txt" 1  \n "simple:0.txt" 1    -->
	 * "MapReduce" "0.txt:1"  \n  "is" "0.txt:1"  \n "simple" "0.txt:1"
	 */
	static class InvertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
		Text outputKey = new Text();
		Text outputValue = new Text();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			try {
				String[] strs = key.toString().split(":");
				
				int sum =0;
				for (Text value : values) {
					sum+=Integer.parseInt(value.toString());
				}
				
				outputKey .set(strs[0]);
				outputValue.set(strs[1] + ":" + sum);
				context.write(outputKey, outputValue);
			} catch (Exception e) {
				context.getCounter(counter.LINESKIP).increment(1);//源文件格式错误，则跳过，计数器+1
				return;
			}
		}
	}
	
	/**
	 * 生成文档列表
	 * "MapReduce" "0.txt:1"  \n  "is" "0.txt:1"  \n  "is" "1.txt:1"  -->
	 * "MapReduce" "0.txt:1"  \n  "is" "0.txt:1, 1.txt:1"
	 */
	static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
		Text outputValue = new Text();
		
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			try {
				String document = new String();
				for (Text value : values) {
					
					document += value.toString() + ";";
				}
				outputValue.set(document);
				context.write(key, outputValue);
			} catch (Exception e) {
				context.getCounter(counter.LINESKIP).increment(1);//源文件格式错误，则跳过，计数器+1
				return;
			}
			
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if(otherArgs.length != 2){
			System.err.println("Usage: InvertedIndex <in> <out>");
			System.exit(2);
		}
		
		Job job = new Job(conf, InvertedIndex.class.getSimpleName());
		
		job.setJarByClass(InvertedIndex.class);
		
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
	
	public static void main(String[] args) throws Exception {
		args = new String[]{
				"hdfs://master:9000/user/hadoop/InvertedIdx/",
				"hdfs://master:9000/user/hadoop/InvertedIdxOut"
		};
		
		int status = new InvertedIndex().run(args);
		System.exit(status);
	}
}
