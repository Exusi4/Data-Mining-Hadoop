package com.eecs498;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
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
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class PageRank {

    public static class DegMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text text, Mapper.Context context) throws IOException, InterruptedException {

            String inputTextasString = text.toString();
            String[] tokenizedString = inputTextasString.split("\\s");
            context.write(new Text(tokenizedString[0]), new IntWritable(1));

        }
    }

    public static class MyMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

        public void map(LongWritable key, Text text, Mapper.Context context) throws IOException, InterruptedException {

            String inputTextasString = text.toString();
            String[] tokenizedString = inputTextasString.split("\\s");
            int index = Integer.valueOf(tokenizedString[0]);
            float out = 1;
            context.write(new Text(tokenizedString[1]), new FloatWritable(out / deg.size() / deg.get(index)));

        }
    }

    public static class MyMapperLoop extends Mapper<LongWritable, Text, Text, FloatWritable> {

        private static Map<Integer, Float> values = new HashMap<Integer, Float>();

        public void setup(Context context) throws IOException {

            values.clear();
            URI[] cacheFiles = context.getCacheFiles();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path path = new Path(cacheFiles[0].toString());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
            String eachLine;
            while ((eachLine = reader.readLine()) != null) {
                String[] tmp = eachLine.split(",");
                values.put(Integer.valueOf(tmp[0]), Float.valueOf(tmp[1]));
            }
        }

        public void map(LongWritable key, Text text, Mapper.Context context) throws IOException, InterruptedException {

            String inputTextasString = text.toString();
            String[] tokenizedString = inputTextasString.split("\\s");
            int index = Integer.valueOf(tokenizedString[0]);
            context.write(new Text(tokenizedString[1]), new FloatWritable(values.get(index) / deg.get(index)));

        }
    }

    public static class DegReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for(IntWritable value: values) sum += value.get();

            String index = key.toString();
            deg.put(Integer.valueOf(index), sum);
            context.write(key, new IntWritable(sum));

        }
    }

    public static class MyReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

            float sum = 0;
            for(FloatWritable value: values) sum += value.get();
            context.write(key, new FloatWritable((1-B)/deg.size() + B*sum));

        }
    }

    private static String inputPath;
    private static String outputScheme;
    private static int T;
    private static float B;
    private static Map<Integer, Integer> deg = new HashMap<Integer, Integer>();

    public static void main(String[] args) throws Exception {

        for(int i = 0; i < args.length; ++i) {
            if (args[i].equals("--input_path")) {
                inputPath = args[++i];
            } else if (args[i].equals("--outputScheme")) {
                outputScheme = args[++i];
            } else if (args[i].equals("-T")) {
                T = Integer.valueOf(args[++i]);
            } else if (args[i].equals("-B")) {
                B = Float.valueOf(args[++i]);
            } else {
                throw new IllegalArgumentException("Illegal cmd line arguement");
            }
        }

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("mapreduce.job.queuename", "eecs498f18");

        Job DegJob = Job.getInstance(conf, "DegJob");
        DegJob.setJarByClass(PageRank.class);
        DegJob.setMapperClass(DegMapper.class);
        DegJob.setReducerClass(DegReducer.class);

        DegJob.setMapOutputKeyClass(Text.class);
        DegJob.setMapOutputValueClass(IntWritable.class);
        DegJob.setOutputKeyClass(Text.class);
        DegJob.setOutputValueClass(IntWritable.class);
        DegJob.setInputFormatClass(TextInputFormat.class);
        DegJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(DegJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(DegJob, new Path(String.format("%s%d", outputScheme, 0)));
        DegJob.waitForCompletion(true);

        Job PageRankJob = Job.getInstance(conf, "PageRankJob");
        PageRankJob.setJarByClass(PageRank.class);
        PageRankJob.setMapperClass(MyMapper.class);
        PageRankJob.setReducerClass(MyReducer.class);

        PageRankJob.setMapOutputKeyClass(Text.class);
        PageRankJob.setMapOutputValueClass(FloatWritable.class);
        PageRankJob.setOutputKeyClass(Text.class);
        PageRankJob.setOutputValueClass(FloatWritable.class);
        PageRankJob.setInputFormatClass(TextInputFormat.class);
        PageRankJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(PageRankJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(PageRankJob, new Path(String.format("%s%d", outputScheme, 1)));
        PageRankJob.waitForCompletion(true);

        for (int i = 1; i < T; i++) {

            Job PageRankJobLoop = Job.getInstance(conf, "PageRankJobLoop");
            PageRankJobLoop.setJarByClass(PageRank.class);
            PageRankJobLoop.setMapperClass(MyMapperLoop.class);
            PageRankJobLoop.setReducerClass(MyReducer.class);

            PageRankJobLoop.addCacheFile(new Path(outputScheme + Integer.toString(i) + "/part-r-00000").toUri());

            PageRankJobLoop.setMapOutputKeyClass(Text.class);
            PageRankJobLoop.setMapOutputValueClass(FloatWritable.class);
            PageRankJobLoop.setOutputKeyClass(Text.class);
            PageRankJobLoop.setOutputValueClass(FloatWritable.class);
            PageRankJobLoop.setInputFormatClass(TextInputFormat.class);
            PageRankJobLoop.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(PageRankJobLoop, new Path(inputPath));
            FileOutputFormat.setOutputPath(PageRankJobLoop, new Path(String.format("%s%d", outputScheme, i+1)));
            PageRankJobLoop.waitForCompletion(true);

        }
    }
}
