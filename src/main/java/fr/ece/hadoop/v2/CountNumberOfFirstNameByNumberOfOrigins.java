package fr.ece.hadoop.v2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class CountNumberOfFirstNameByNumberOfOrigins {
    private static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        //In this map the key is the number of origins of the name and the value is one
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] lineSplit = line.split(";");
            String[] originSplit = lineSplit[2].split(",");
            if (originSplit[0].isEmpty()) {
                context.write(new IntWritable(0), one);
            } else {
                context.write(new IntWritable(originSplit.length), one);
            }
        }
    }

    private static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        //In the reduce we sum the values associated with the key.
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setJarByClass(CountNameByOrigin.class);

        job.setJobName("fr.ece.hadoop.v2.CountNumberOfFirstNameByNumberOfOrigins");

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.waitForCompletion(true);
    }

}
