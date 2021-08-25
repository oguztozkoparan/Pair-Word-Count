package com.inprobe.wordcount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class Driver{

    private static Logger logger = Logger.getLogger(Driver.class);

    public static void main(String[] args) throws Exception{
        
        // Debugging!
        System.out.println("ccccc");

        JobConf conf = new JobConf();
        Job job = new Job(conf, "com/inprobe/wordcount");

        // https://hadoop.apache.org/docs/r2.7.3/api/org/apache/hadoop/mapreduce/Job.html
        job.setJarByClass(Driver.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
