package com.eecs498;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class FrequentItemsets {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text text, Mapper.Context context) throws IOException, InterruptedException {

            List<String> raw_items = Arrays.asList(text.toString().split(","));
            List<String> items = new ArrayList(raw_items);
            items.remove(0);

            for (int i = 0; i < items.size(); i++) {
                context.write(new Text(items.get(i)), one);
            }
        }
    }

    public static class MyMapperLoop extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static IntWritable one = new IntWritable(1);
        private static List<String> generated = new ArrayList();
        private static List<String> singleton = new ArrayList();
        private static List<String> k_ton = new ArrayList();

        public void setup(Context context) throws IOException {

            generated.clear();
            singleton.clear();
            k_ton.clear();

            URI[] cacheFiles = context.getCacheFiles();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path path = new Path(cacheFiles[0].toString());
            Path path1 = new Path(cacheFiles[1].toString());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
            BufferedReader reader1 = new BufferedReader(new InputStreamReader(fs.open(path1)));
            String eachLine;
            while ((eachLine = reader.readLine()) != null) {
                String[] Id = eachLine.split(",");
                singleton.add(Id[0]);
            }
            while ((eachLine = reader1.readLine()) != null) {
                String Ids = "";
                String[] eachId = eachLine.split(",");
                for (int i = 0; i < c; i++) {
                    Ids = Ids + eachId[i] + ",";
                }
                Ids = Ids.substring(0, Ids.length() - 1);
                k_ton.add(Ids);
            }

            for (String kton : k_ton) {
                String[] eachId = kton.split(",");
                for (String single : singleton) {
                    if (Integer.valueOf(single) > Integer.valueOf(eachId[c-1])) {
                        generated.add(kton + "," + single);
                    }
                }
            }
        }

        public void map(LongWritable key, Text text, Mapper.Context context) throws IOException, InterruptedException {

            List<String> raw_items = Arrays.asList(text.toString().split(","));
            List<String> items = new ArrayList(raw_items);
            items.remove(0);
            for (String s: generated){

                int t = 0;
                String[] generatedIds = s.split(",");
                for (int i = 0; i <= c; i++) {
                    if (items.contains(generatedIds[i])) t++;
                }

                if (t == c + 1) {context.write(new Text(s), one);}
            }
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for(IntWritable value: values) {
                sum += value.get();
            }
            if (sum >= s) {
                context.write(key, new IntWritable(sum));
            }
        }
    }

    private static String outputScheme;
    private static String ratingsFile;
    private static int s;
    private static int k;
    private static int c = 0;

    public static void main(String[] args) throws Exception {

        for (int i = 0; i < args.length; ++i) {
            if (args[i].equals("--ratingsFile")) {
                ratingsFile = args[++i];
            } else if (args[i].equals("-k")) {
                k = Integer.valueOf(args[++i]);
            } else if (args[i].equals("-s")) {
                s = Integer.valueOf(args[++i]);
            } else if (args[i].equals("--outputScheme")) {
                outputScheme = args[++i];
            } else {
                throw new IllegalArgumentException("Illegal cmd line arguement");
            }
        }

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job FrequentItemsetsJob = Job.getInstance(conf, "FrequentItemsetsJob");

        FrequentItemsetsJob.setJarByClass(FrequentItemsets.class);
        FrequentItemsetsJob.setMapperClass(MyMapper.class);
        FrequentItemsetsJob.setReducerClass(MyReducer.class);

        FrequentItemsetsJob.setMapOutputKeyClass(Text.class);
        FrequentItemsetsJob.setMapOutputValueClass(IntWritable.class);
        FrequentItemsetsJob.setOutputKeyClass(Text.class);
        FrequentItemsetsJob.setOutputValueClass(IntWritable.class);
        FrequentItemsetsJob.setInputFormatClass(TextInputFormat.class);
        FrequentItemsetsJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(FrequentItemsetsJob, new Path(ratingsFile));
        FileOutputFormat.setOutputPath(FrequentItemsetsJob, new Path(outputScheme));
        FrequentItemsetsJob.waitForCompletion(true);

        for (int i = 1; i < k; i++) {

            c++;

            Job FrequentItemsetsLoopJob = Job.getInstance(conf, "FrequentItemsetsLoopJob");

            FrequentItemsetsLoopJob.setJarByClass(FrequentItemsets.class);
            FrequentItemsetsLoopJob.setMapperClass(MyMapperLoop.class);
            FrequentItemsetsLoopJob.setReducerClass(MyReducer.class);

            if (i == 1) {
                FrequentItemsetsLoopJob.addCacheFile(new Path(outputScheme + "/part-r-00000").toUri());
                FrequentItemsetsLoopJob.addCacheFile(new Path(outputScheme + "/part-r-00000").toUri());
            }
            else {
                FrequentItemsetsLoopJob.addCacheFile(new Path(outputScheme + "/part-r-00000").toUri());
                FrequentItemsetsLoopJob.addCacheFile(new Path(outputScheme + Integer.toString(i-1) + "/part-r-00000").toUri());
            }

            FrequentItemsetsLoopJob.setMapOutputKeyClass(Text.class);
            FrequentItemsetsLoopJob.setMapOutputValueClass(IntWritable.class);
            FrequentItemsetsLoopJob.setOutputKeyClass(Text.class);
            FrequentItemsetsLoopJob.setOutputValueClass(IntWritable.class);
            FrequentItemsetsLoopJob.setInputFormatClass(TextInputFormat.class);
            FrequentItemsetsLoopJob.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(FrequentItemsetsLoopJob, new Path(ratingsFile));
            FileOutputFormat.setOutputPath(FrequentItemsetsLoopJob, new Path(String.format("%s%d", outputScheme, i)));
            FrequentItemsetsLoopJob.waitForCompletion(true);

        }
    }
}