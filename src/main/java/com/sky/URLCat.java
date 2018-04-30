package com.sky;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class URLCat{
	//map将输入中的value转换成IntWritable类型，作为输出的key
	public class Map extends Mapper<Object,Text,Text,IntWritable>{
		   private final IntWritable one = new IntWritable(1);
		   private Text word = new Text();
		   
		   public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		     StringTokenizer itr = new StringTokenizer(value.toString());
		     while (itr.hasMoreTokens()) {
		       word.set(itr.nextToken());
		       context.write(word, one);
		     }
		   }
	}
	
	public static class Reduce<Key> extends Reducer<Key,IntWritable,
	Key,IntWritable> {
	private IntWritable result = new IntWritable();

	public void reduce(Key key, Iterable<IntWritable> values,
	Context context) throws IOException, InterruptedException {
	int sum = 0;
	for (IntWritable val:values) {
	sum += val.get();
	}
	result.set(sum);
	context.write(key, result);
	}
	}	
	
	public static void main(String[] args)throws Exception{
		Configuration conf=new Configuration();
		String[] ioArgs=new String[]{"sort_in","sort_out"};
		String[] otherArgs=new GenericOptionsParser(conf,ioArgs).getRemainingArgs();
		if(otherArgs.length!=2){
			System.err.println("Usage:Data sort <in> <out>");
			System.exit(2);
		}
		
		Job job=Job.getInstance(conf,"skyjob:Data Sort");
		job.setJarByClass(com.sky.URLCat.class);
		
		//设置Map和Reduce处理类
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		//设置输出类型
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		//设置输入和输出目录
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));		
		try{
			job.waitForCompletion(true);
			System.out.print("成功");
			System.exit(0);
		}catch(Exception e){
			System.out.print("失败");
			System.exit(1);			
		}			
	}
	
}


