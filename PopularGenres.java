package com.eecs498;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
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
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class PopularGenres {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

        private static Map<Integer, String> Id2genre = new HashMap<Integer, String>();

        public void setup(Context context) throws IOException{

            URI[] cacheFiles = context.getCacheFiles();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path path = new Path(cacheFiles[0].toString());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
            String eachLine;
            int Id;
            String genre;
            while ((eachLine = reader.readLine()) != null) {
                String[] eachId = eachLine.split(",");
                Id = Integer.valueOf(eachId[0]);
                genre = eachId[1];
                for (int i = 2; i < eachId.length; i++){
                    genre = genre + "," + eachId[i];
                }
                Id2genre.put(Id, genre);
            }
        }

        public void map(LongWritable key, Text text, Mapper.Context context) throws IOException, InterruptedException {

            String inputTextasString = text.toString();
            String[] tokenizedString = inputTextasString.split(",");
            FloatWritable score = new FloatWritable(Float.valueOf(tokenizedString[2]));
            Text genreAndyear = new Text();

            long u_time = Long.valueOf(tokenizedString[3]);

            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(u_time*1000L);
            int beginningyear = cal.get(Calendar.YEAR) / 5;
            beginningyear = beginningyear * 5;

            String raw_genre = Id2genre.get(Integer.valueOf(tokenizedString[1]));
            String[] genres = raw_genre.split(",");
            for (int i = 0; i < genres.length; i++){
                genreAndyear.set(genres[i].trim() + "," + Integer.toString(beginningyear));
                context.write(genreAndyear, score);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {

            FloatWritable avgScore = new FloatWritable();
            float avg = 0;
            int count = 0;
            for (FloatWritable val : values) {
                avg += val.get();
                count++;
            }
            avg = avg / count;
            avgScore.set(avg);
            context.write(key, avgScore);
        }
    }

    private static String ratingsFile;
    private static String genresFile;
    private static String outputScheme;

    public static void main(String[] args) throws Exception {

        for(int i = 0; i < args.length; ++i) {
            if (args[i].equals("--ratingsFile")) {
                ratingsFile = args[++i];
            } else if (args[i].equals("--genresFile")) {
                genresFile = args[++i];
            } else if (args[i].equals("--outputScheme")) {
                outputScheme = args[++i];
            } else {
                throw new IllegalArgumentException("Illegal cmd line arguement");
            }
        }

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job PopularGenresJob = Job.getInstance(conf, "PopularGenresJob");

        PopularGenresJob.setJarByClass(PopularGenres.class);
        PopularGenresJob.setMapperClass(MyMapper.class);
        PopularGenresJob.setReducerClass(MyReducer.class);

        PopularGenresJob.addCacheFile(new Path(genresFile).toUri());

        PopularGenresJob.setMapOutputKeyClass(Text.class);
        PopularGenresJob.setMapOutputValueClass(FloatWritable.class);
        PopularGenresJob.setOutputKeyClass(Text.class);
        PopularGenresJob.setOutputValueClass(FloatWritable.class);
        PopularGenresJob.setInputFormatClass(TextInputFormat.class);
        PopularGenresJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(PopularGenresJob, new Path(ratingsFile));
        FileOutputFormat.setOutputPath(PopularGenresJob, new Path(outputScheme));
        PopularGenresJob.waitForCompletion(true);

    }
}
