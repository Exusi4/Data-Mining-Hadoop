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

public class UserPreference {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text text, Mapper.Context context) throws IOException, InterruptedException {

            String inputTextasString = text.toString();
            String[] tokenizedString = inputTextasString.split(",");

            Text userId = new Text(tokenizedString[0]);
            IntWritable Preference = new IntWritable();
            Preference.set(0);
            context.write(userId, Preference);
            double sum = 0;
            double score = 0;
            int count = 0;

            for (int i = 1; i < tokenizedString.length; i++){
                score = Double.valueOf(tokenizedString[i]);
                if (score > 0) {
                    sum += score;
                    count++;
                }
            }

            if (count > 0) {
                sum = sum / count;
                for (int i = 1; i < tokenizedString.length; i++){
                    score = Double.valueOf(tokenizedString[i]);
                    if (score > sum) {
                        Preference.set(1);
                        context.write(userId, Preference);
                    }
                }
            }
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            IntWritable moviecount = new IntWritable();
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            moviecount.set(sum);
            context.write(key, moviecount);

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

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job UserPreferenceJob = Job.getInstance(conf, "UserPreferenceJob");

        UserPreferenceJob.setJarByClass(UserPreference.class);
        UserPreferenceJob.setMapperClass(MyMapper.class);
        UserPreferenceJob.setReducerClass(MyReducer.class);

        UserPreferenceJob.setMapOutputValueClass(IntWritable.class);
        UserPreferenceJob.setMapOutputKeyClass(Text.class);
        UserPreferenceJob.setOutputValueClass(IntWritable.class);
        UserPreferenceJob.setOutputKeyClass(Text.class);
        UserPreferenceJob.setInputFormatClass(TextInputFormat.class);
        UserPreferenceJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(UserPreferenceJob, new Path(ratingsFile));
        FileOutputFormat.setOutputPath(UserPreferenceJob, new Path(outputScheme));
        UserPreferenceJob.waitForCompletion(true);

    }
}
