package com.eecs498;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class AssociationRules {

    public static class MyMapper1 extends Mapper<LongWritable, Text, Text, IntWritable> {

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

    public static class MyMapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static IntWritable one = new IntWritable(1);
        private static List<String> generated = new ArrayList();
        private static List<String> singleton = new ArrayList();

        public void setup(Context context) throws IOException {

            generated.clear();
            singleton.clear();

            URI[] cacheFiles = context.getCacheFiles();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path path = new Path(cacheFiles[0].toString());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
            String eachLine;
            while ((eachLine = reader.readLine()) != null) {
                String[] Id = eachLine.split(",");
                singleton.add(Id[0]);
            }

            for (String singLe : singleton) {
                for (String single : singleton) {
                    if (Integer.valueOf(single) > Integer.valueOf(singLe)) {
                        generated.add(singLe + "," + single);
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
                for (int i = 0; i <= 1; i++) {
                    if (items.contains(generatedIds[i])) t++;
                }

                if (t == 2) {context.write(new Text(s), one);}
            }
        }
    }

    public static class MyReducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for(IntWritable value: values) {
                sum += value.get();
            }
            if (sum >= s){
                context.write(key, new IntWritable(sum));
            }
        }
    }

    public static class MyReducer2 extends Reducer<Text, IntWritable, Text, FloatWritable> {

        private static Map<String, Integer> Id2sup = new HashMap<String, Integer>();

        public void setup(Context context) throws IOException {

            URI[] cacheFiles = context.getCacheFiles();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path path = new Path(cacheFiles[0].toString());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
            String eachLine;
            String Id;
            int sup;
            while ((eachLine = reader.readLine()) != null) {
                String[] eachId = eachLine.split(",");
                Id = eachId[0].trim();
                sup = Integer.valueOf(eachId[1]);
                Id2sup.put(Id, sup);
            }
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            Stream<IntWritable> valuesStream = StreamSupport.stream(values.spliterator(), false);
            Optional<Integer> support = valuesStream.map(v -> v.get()).reduce(Integer::sum);
            FloatWritable conf = new FloatWritable();
            Text alteredkey = new Text();

            if(support.get() >= s) {

                String IdasString = key.toString();
                String[] Ids = IdasString.split(",");

                conf.set((float)support.get()/Id2sup.get(Ids[0]));
                alteredkey.set(Ids[0] + "->" + Ids[1]);
                context.write(alteredkey, conf);

                conf.set((float)support.get()/Id2sup.get(Ids[1]));
                alteredkey.set(Ids[1] + "->" + Ids[0]);
                context.write(alteredkey, conf);

            }
        }
    }

    private static String outputScheme;
    private static String ratingsFile;
    private static int s;
    private static int k;

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

        k = 2;

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job AssociationRulesJob1 = Job.getInstance(conf, "AssociationRulesJob1");

        AssociationRulesJob1.setJarByClass(AssociationRules.class);
        AssociationRulesJob1.setMapperClass(MyMapper1.class);
        AssociationRulesJob1.setReducerClass(MyReducer1.class);

        AssociationRulesJob1.setMapOutputKeyClass(Text.class);
        AssociationRulesJob1.setMapOutputValueClass(IntWritable.class);
        AssociationRulesJob1.setOutputKeyClass(Text.class);
        AssociationRulesJob1.setOutputValueClass(IntWritable.class);
        AssociationRulesJob1.setInputFormatClass(TextInputFormat.class);
        AssociationRulesJob1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(AssociationRulesJob1, new Path(ratingsFile));
        FileOutputFormat.setOutputPath(AssociationRulesJob1, new Path(outputScheme));
        AssociationRulesJob1.waitForCompletion(true);

        Job AssociationRulesJob2 = Job.getInstance(conf, "AssociationRulesJob2");
        AssociationRulesJob2.setJarByClass(AssociationRules.class);
        AssociationRulesJob2.setMapperClass(MyMapper2.class);
        AssociationRulesJob2.setReducerClass(MyReducer2.class);

        AssociationRulesJob2.addCacheFile(new Path(outputScheme + "/part-r-00000").toUri());

        AssociationRulesJob2.setMapOutputKeyClass(Text.class);
        AssociationRulesJob2.setMapOutputValueClass(IntWritable.class);
        AssociationRulesJob2.setOutputKeyClass(Text.class);
        AssociationRulesJob2.setOutputValueClass(FloatWritable.class);
        AssociationRulesJob2.setInputFormatClass(TextInputFormat.class);
        AssociationRulesJob2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(AssociationRulesJob2, new Path(ratingsFile));
        FileOutputFormat.setOutputPath(AssociationRulesJob2, new Path(String.format("%s%d", outputScheme, 1)));
        AssociationRulesJob2.waitForCompletion(true);

    }
}