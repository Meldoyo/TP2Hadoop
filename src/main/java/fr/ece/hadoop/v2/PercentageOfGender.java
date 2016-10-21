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

        //In this map, the key is the gender (as a Text) and the value is one
        //We increment a counter each time we map a value in order to calculate the percentage in the reduce
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
        // We obtain the value of the input counter from the mapper in the reducer.
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Cluster cluster = new Cluster(conf);
            Job currentJob = cluster.getJob(context.getJobID());
            mapperCounter = currentJob.getCounters().findCounter("Custom counter", "Input counter").getValue();
        }


        //This reduce sum the values for each key
        //We then calculate the percentage using the value of the counter
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            float percent = sum * 100.0f / mapperCounter;

            context.write(key, new IntWritable((int)percent));
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
        job.setReducerClass(Reduce.class);

        job.waitForCompletion(true);
    }

}
