package com.sky;
import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class CatSpark{
	
	public static void main(String[] args)throws Exception{
	SparkConf conf=new SparkConf().setMaster("local").setAppName("Spark App");
	JavaSparkContext sc=new JavaSparkContext(conf);
	//读取输入的数据
	JavaRDD<String> lines=sc.textFile("README.md");
	//切分为单词
	JavaRDD<String> words=lines.flatMap(new FlatMapFunction<String,String>(){
		public Iterable<String> call(String x){
			return Arrays.asList(x.split(" "));
		}
	});
	//转换为键值对并计数
	JavaPairRDD<String,Integer> counts=words.mapToPair(new PairFunction<String,String,Integer>(){
		public Tuple2<String,Integer> call(String x){
			return new Tuple2(x,1);
		}
	}).reduceByKey(new Function2<Integer,Integer,Integer>(){
		public Integer call(Integer x,Integer y){
			return x+y;
		}
	});
	//System.out.println(counts);
	counts.saveAsTextFile("countall");
	System.out.print("\n");
	JavaRDD<String> errorsRDD=lines.filter(
			new Function<String,Boolean>(){
				public Boolean call(String x){
					return x.contains("error");
				}
	});
	//System.out.println(errorsRDD);
	errorsRDD.saveAsTextFile("errorall");
	
	
	
	}	
	
}