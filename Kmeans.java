package com.eecs498;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.lang.Math;
import java.net.URI;
import java.util.*;

public class Kmeans {

    public static class MyMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private static List<String> centroids = new ArrayList<>();

        public void setup(Context context) throws IOException {

            centroids.clear();
            URI[] cacheFiles = context.getCacheFiles();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path path = new Path(cacheFiles[0].toString());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
            String eachLine;
            int tmp = 0;
            while ((eachLine = reader.readLine()) != null && tmp < k) {
                centroids.add(eachLine);
                tmp++;
            }
        }

        public void map(LongWritable key, Text text, Mapper.Context context) throws IOException, InterruptedException {

            String inputTextasString = text.toString();
            String[] tokenizedString = inputTextasString.split(",");
            d = tokenizedString.length;
            float[] datum = new float[d];
            for (int i = 0; i < d; i++) datum[i] = Float.valueOf(tokenizedString[i]);

            int cluster = 0;
            float min_distance = 0;
            float min_centroid_norm = 0;
            int i = 0;

            for (String centroid : centroids) {

                String[] tokenizedString_2 = centroid.split(",");
                float[] cen = new float[d];
                float distance = 0;
                float centroid_norm = 0;

                for (int j = 0; j < d; j++) {
                    cen[j] = Float.valueOf(tokenizedString_2[j]);
                    centroid_norm += Math.pow(cen[j], 2);
                    if (norm.equals("2")) distance += Math.pow(datum[j] - cen[j], 2);
                    else distance += Math.abs(datum[j] - cen[j]);
                }

                if (distance < min_distance || i == 0) {

                    min_distance = distance;
                    cluster = i;
                    min_centroid_norm = centroid_norm;
                }
                else if (distance == min_distance && centroid_norm < min_centroid_norm) {
                    cluster = i;
                    min_centroid_norm = centroid_norm;
                }
                i++;

            }

            cost_by_iteration += min_distance;

            context.write(new IntWritable(cluster), text);

        }
    }

    public static class MyReducer extends Reducer<IntWritable, Text, Text, NullWritable> {

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            float[] new_centroid = new float[d];

            int flag = 1;

            if (flag == 1) {

                int j = 0;

                for(Text value: values) {
                    String inputTextasString = value.toString();
                    String[] tokenizedString = inputTextasString.split(",");
                    float[] datum = new float[d];
                    for (int i = 0; i < d; i++) {
                        datum[i] = Float.valueOf(tokenizedString[i]);
                        if (j == 0) new_centroid[i] = datum[i];
                        else new_centroid[i] += datum[i];
                    }
                    j++;
                }
                for (int i = 0; i < d; i++) {
                    new_centroid[i] = new_centroid[i]/j;
                }
            }

            else {
                String[] d_string = new String[d];
                int tmp = 0;
                for (Text value : values) {
                    String inputTextasString = value.toString();
                    String[] tokenizedString = inputTextasString.split(",");
                    if (tmp == 0) {
                        for (int i = 0; i < d; i++) d_string[i] = tokenizedString[i];
                        tmp++;
                    }
                    else{
                        for (int i = 0; i < d; i++) d_string[i] = d_string[i] + "," + tokenizedString[i];
                    }
                }
                for (int i = 0; i < d; i++) {

                    ArrayList<Float> list = new ArrayList<Float>();
                    String[] tokenizedString = d_string[i].split(",");
                    int len = 0;
                    for (String s : tokenizedString) {
                        list.add(Float.valueOf(s));
                        len++;
                    }
                    Collections.sort(list);
                    if (len % 2 == 0){
                        new_centroid[i] = (list.get(len/2-1)+list.get(len/2))/2;
                    }
                    else {
                        new_centroid[i] = list.get(len/2);
                    }
                }
            }

            String text = "";
            for (int i = 0; i < d; i++) text = text + Float.toString(new_centroid[i]) + ",";
            text = text.substring(0, text.length()-1);
            context.write(new Text(text), NullWritable.get());

        }
    }

    private static String inputPath;
    private static String centroidPath;
    private static String outputScheme;
    private static String norm;
    private static int k;
    private static int n;
    private static int d;
    private static float cost_by_iteration = 0;

    public static void main(String[] args) throws Exception {

        for(int i = 0; i < args.length; ++i) {

            if (args[i].equals("--inputPath")) {
                inputPath = args[++i];
            } else if (args[i].equals("--centroidPath")) {
                centroidPath = args[++i];
            } else if (args[i].equals("--outputScheme")) {
                outputScheme = args[++i];
            } else if (args[i].equals("--norm")) {
                norm = args[++i];
            } else if (args[i].equals("-k")) {
                k = Integer.valueOf(args[++i]);
            } else if (args[i].equals("-n")) {
                n = Integer.valueOf(args[++i]);
            } else {
                throw new IllegalArgumentException("Illegal cmd line arguement");
            }
        }

        File csv = new File(centroidPath + ".csv");
        BufferedWriter bw = new BufferedWriter(new FileWriter(csv, true));

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        conf.set("mapreduce.job.queuename", "eecs498f18");

        for (int i = 1; i <= n; i++) {

            Job KmeansJob = Job.getInstance(conf, "KmeansJob");
            KmeansJob.setJarByClass(Kmeans.class);
            KmeansJob.setMapperClass(MyMapper.class);
            KmeansJob.setReducerClass(MyReducer.class);

            if (i == 1) {
                KmeansJob.addCacheFile(new Path(centroidPath).toUri());
            }
            else {
                KmeansJob.addCacheFile(new Path(outputScheme + Integer.toString(i-1) + "/part-r-00000").toUri());
            }

            KmeansJob.setMapOutputKeyClass(IntWritable.class);
            KmeansJob.setMapOutputValueClass(Text.class);
            KmeansJob.setOutputKeyClass(Text.class);
            KmeansJob.setOutputValueClass(NullWritable.class);
            KmeansJob.setInputFormatClass(TextInputFormat.class);
            KmeansJob.setOutputFormatClass(TextOutputFormat.class);

            FileInputFormat.addInputPath(KmeansJob, new Path(inputPath));
            FileOutputFormat.setOutputPath(KmeansJob, new Path(String.format("%s%d", outputScheme, i)));
            KmeansJob.waitForCompletion(true);

            bw.write(Float.toString(cost_by_iteration));
            bw.newLine();
            cost_by_iteration = 0;
        }

        bw.close();
    }
}
