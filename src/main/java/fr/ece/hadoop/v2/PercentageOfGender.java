package fr.ece.hadoop.v2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by pcordonnier on 20/10/16.
 */
public class PercentageOfGender {
    private static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] lineSplit = line.split(";");
            String[] genderSplit = lineSplit[1].split(",");
            if (genderSplit.length == 1) {
                context.write(new Text(genderSplit[0].startsWith("m") ? "male" : "female"), one);
                context.getCounter("Custom counter", "Input counter").increment(1);
            }
            //It makes no sense to add to both female and male is name is both since we are doing percentage.
        }
    }

    private static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        private long mapperCounter;

        // http://stackoverflow.com/questions/5450290/accessing-a-mappers-counter-from-a-reducer
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Cluster cluster = new Cluster(conf);
            Job currentJob = cluster.getJob(context.getJobID());
            mapperCounter = currentJob.getCounters().findCounter("Custom counter", "Input counter").getValue();
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            //int percent = (float)((float)sum * 100.0f/ mapperCounter);

            //What I was expecting to do:
            //From a counter in the map phase, I get the number of total lines I have, which the total number of names
            //Through the setup, I obtain this value in the reduce
            //Now I just need to divide the sum of male by the total by the total !
            //Except NO, reduce is called twice per key, which I don't understand why, and I can't calculate percentage this way since the calculation is done twice
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(CountNameByOrigin.class);

        job.setJobName("fr.ece.hadoop.v2.PercentageOfGender");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.waitForCompletion(true);
    }

}
