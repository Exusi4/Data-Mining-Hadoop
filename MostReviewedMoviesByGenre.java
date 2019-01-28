package com.eecs498;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
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
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class MostReviewedMoviesByGenre {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private static Map<Integer, String> Id2genre = new HashMap<Integer, String>();


        public void setup(Context context) throws IOException {

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
            IntWritable movieId = new IntWritable(Integer.valueOf(tokenizedString[1]));
            Text yearAndgenre = new Text();

            long u_time = Long.valueOf(tokenizedString[3]);

            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(u_time*1000L);
            int beginningyear = cal.get(Calendar.YEAR) / 5;
            beginningyear = beginningyear*5;

            String raw_genre = Id2genre.get(Integer.valueOf(tokenizedString[1]));
            String[] genres = raw_genre.split(",");
            for (int i = 0; i < genres.length; i++){
                yearAndgenre.set(Integer.toString(beginningyear) + "," + genres[i].trim());
                context.write(yearAndgenre, movieId);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, Text> {

        private static Map<Integer, String> Id2name = new HashMap<Integer, String>();

        public void setup(Context context) throws IOException{

            URI[] cacheFiles = context.getCacheFiles();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            Path path = new Path(cacheFiles[1].toString());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
            String eachLine;
            int Id;
            String name;
            while ((eachLine = reader.readLine()) != null) {
                String[] eachId = eachLine.split(",");
                Id = Integer.valueOf(eachId[0]);
                name = eachId[1];
                for (int i = 2; i < eachId.length; i++){
                    name = name + "," + eachId[i];
                }
                Id2name.put(Id, name);
            }
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            Text countIdname = new Text();
            int countMAX = 0;
            int movieId = 1;
            Map<Integer, Integer> Id2mode = new HashMap<Integer, Integer>();

            for (IntWritable val : values) {
                if (Id2mode.containsKey(val.get())) {
                    Id2mode.put(val.get(), Id2mode.get(val.get()) + 1);
                }
                else {
                    Id2mode.put(val.get(),1);
                }
            }

            for (int myId : Id2mode.keySet()) {

                if (Id2mode.get(myId) > countMAX) {
                    movieId = myId;
                    countMAX = Id2mode.get(myId);
                }
                else if (Id2mode.get(myId) == countMAX) {

                    ArrayList<String> list = new ArrayList<String>();
                    list.add(Id2name.get(movieId));
                    list.add(Id2name.get(myId));
                    Collections.sort(list);
                    if (Id2name.get(myId) == list.get(0)) {
                        movieId = myId;
                    }
                }
            }

            countIdname.set(Integer.toString(countMAX) + "," + Integer.toString(movieId) + "," + Id2name.get(movieId));
            context.write(key, countIdname);
        }
    }

    private static String ratingsFile;
    private static String genresFile;
    private static String outputScheme;
    private static String movieNameFile;

    public static void main(String[] args) throws Exception {

        for(int i = 0; i < args.length; ++i) {

            if (args[i].equals("--ratingsFile")) {
                ratingsFile = args[++i];
            } else if (args[i].equals("--genresFile")) {
                genresFile = args[++i];
            } else if (args[i].equals("--movieNameFile")) {
                movieNameFile = args[++i];
            } else if (args[i].equals("--outputScheme")) {
                outputScheme = args[++i];
            } else {
                throw new IllegalArgumentException("Illegal cmd line arguement");
            }
        }

        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");

        Job MostReviewedMoviesByGenreJob = Job.getInstance(conf, "MostReviewedMoviesByGenreJob");
        MostReviewedMoviesByGenreJob.setJarByClass(MostReviewedMoviesByGenre.class);
        MostReviewedMoviesByGenreJob.setMapperClass(MyMapper.class);
        MostReviewedMoviesByGenreJob.setReducerClass(MyReducer.class);

        MostReviewedMoviesByGenreJob.addCacheFile(new Path(genresFile).toUri());
        MostReviewedMoviesByGenreJob.addCacheFile(new Path(movieNameFile).toUri());

        MostReviewedMoviesByGenreJob.setMapOutputKeyClass(Text.class);
        MostReviewedMoviesByGenreJob.setMapOutputValueClass(IntWritable.class);
        MostReviewedMoviesByGenreJob.setOutputKeyClass(Text.class);
        MostReviewedMoviesByGenreJob.setOutputValueClass(Text.class);
        MostReviewedMoviesByGenreJob.setInputFormatClass(TextInputFormat.class);
        MostReviewedMoviesByGenreJob.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(MostReviewedMoviesByGenreJob, new Path(ratingsFile));
        FileOutputFormat.setOutputPath(MostReviewedMoviesByGenreJob, new Path(outputScheme));
        MostReviewedMoviesByGenreJob.waitForCompletion(true);
    }
}
