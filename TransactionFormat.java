package com.eecs498;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class TransactionFormat {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text text, Mapper.Context context) throws IOException, InterruptedException {

            String inputTextasString = text.toString();
            String[] tokenizedString = inputTextasString.split(",");
            Text userId = new Text(tokenizedString[0]);
            IntWritable movieId = new IntWritable();
            movieId.set(Integer.valueOf(tokenizedString[1]));
            context.write(userId, movieId);

        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, Text> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            String movies = "";
            for (IntWritable val : values) {
                movies = movies + Integer.toString(val.get()) + ",";
            }
	        movies = movies.substring(0, movies.length() - 1);
            Text movieTuple = new Text(movies);
            context.write(key, movieTuple);

        }
    }

    private static String ratingsFile;
    private static String outputScheme;

    public static void main(String[] args) throws Exception {

        for(int i = 0; i < args.length; ++i) {

            if (args[i].equals("--ratingsFile")) {
                ratingsFile = args[++i];
            } else if (args[i].equals("--outputScheme")) {
                outputScheme = args[++i];
            } else {
                throw new IllegalArgumentException("Illegal cmd line arguement");
            }
        }

        if (ratingsFile  == null || outputScheme == null) {
            throw new RuntimeException("Either output path or input path are not defined");
        }

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job TransactionFormatJob = Job.getInstance(conf, "TransactionFormatJob");

        TransactionFormatJob.setJarByClass(TransactionFormat.class);
        TransactionFormatJob.setMapperClass(MyMapper.class);
        TransactionFormatJob.setReducerClass(MyReducer.class);

        TransactionFormatJob.setMapOutputValueClass(IntWritable.class);
        TransactionFormatJob.setMapOutputKeyClass(Text.class);
        TransactionFormatJob.setOutputValueClass(Text.class);
        TransactionFormatJob.setOutputKeyClass(Text.class);
        TransactionFormatJob.setInputFormatClass(TextInputFormat.class);
        TransactionFormatJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(TransactionFormatJob, new Path(ratingsFile));
        FileOutputFormat.setOutputPath(TransactionFormatJob, new Path(outputScheme));
        TransactionFormatJob.waitForCompletion(true);

    }
}
