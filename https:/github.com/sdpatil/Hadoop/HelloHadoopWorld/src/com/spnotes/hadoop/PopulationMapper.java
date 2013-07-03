package com.spnotes.hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class PopulationMapper extends
org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, IntWritable, LongWritable>{

	@Override
	protected void map(LongWritable key, Text value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
	
		int year = Integer.parseInt(line.substring(0,4));
		
		int population = Integer.parseInt(line.substring(18));
		
		context.write(new IntWritable(year), new LongWritable(population));

		
	}
	
	public static void main(String[] argv) throws Exception{
		BufferedReader fileReader = new BufferedReader(new FileReader(new File("/Users/gpzpati/hadoop/population/ca.1969_2011.19ages.txt")));
		String line = null;
		int counter = 0;
		while( (line= fileReader.readLine()) != null ){
			System.out.println(line);
			String gender =  "Male";
			
			if(line.charAt(16) == '2')
				gender = "Female";
			
			int population = Integer.parseInt(line.substring(17));
			counter ++;
			if(counter == 10)
				break;
			System.out.println("Gender " + gender + " Population " + population);
			
		}
	}

}
