package com.spnotes.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class PopulationReducer extends
Reducer<IntWritable, LongWritable, IntWritable, LongWritable>{

	@Override
	public void reduce(IntWritable key, Iterable<LongWritable> values, Context context)
			throws IOException, InterruptedException {
	
		long totalPopulation = 0;
		for (LongWritable value : values) {
			totalPopulation = totalPopulation + value.get();
		}	
		System.out.println(key.get() + " " + totalPopulation);
		context.write(key, new LongWritable(totalPopulation));

	}

}
