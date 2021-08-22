package com.inprobe.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    public static final IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split("[ \t]+");
        String word = "";
        for(int i = 0; i < words.length-1; i++)
        {
            word = words[i] + " " + words[i+1];
            word = word.replaceAll("[^a-zA-Z0-9 ]", "");

            word = word.toLowerCase();

            Text outKey = new Text(word);
            IntWritable outValue = new IntWritable(1);

            context.write(outKey, outValue);
        }
    }
}
