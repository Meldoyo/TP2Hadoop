import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by pcordonnier on 13/10/16.
 */
public class PercentageOfGender {
    private static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        @Override
        public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String line = text.toString();
            String[] lineSplit = line.split(";");
            String[] genderSplit = lineSplit[1].split(",");
            if (genderSplit.length == 1) {
                outputCollector.collect(new Text(genderSplit[0].startsWith("m") ? "male" : "female"), one);
                reporter.incrCounter("group", "counter", 1);
            }
            //It makes no sense to add to both female and male is name is both since we are doing percentage.
        }
    }

    private static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            int sum = 0;
            while (iterator.hasNext()) {
                sum += iterator.next().get();
            }
            outputCollector.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        JobConf conf1 = new JobConf(PercentageOfGender.class);
        conf1.setJobName("CountNameByGender");
        conf1.setOutputKeyClass(Text.class);
        conf1.setOutputValueClass(IntWritable.class);

        conf1.setMapperClass(Map.class);
        conf1.setCombinerClass(Reduce.class);
        conf1.setReducerClass(Reduce.class);

        conf1.setInputFormat(TextInputFormat.class);
        conf1.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf1, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf1, new Path(args[1]));

        JobClient.runJob(conf1);

        // OK, now we have something like
        // male 5454
        // female 3548
        // We need a second job in order to compute the percentage since
        // we cannot have the total number of key/values during the reduce phase since map is not finished at the start of the reduce.
        // Our goal was to create two jobs, the output of the first would be the input of the second.
        // We can use JobControl in order to add job1 as a depending job of job2.
        // The main problem was to link the two jobs together, we only need to have a reduce phase with an input of the number of names and the total number of names
        // The total numbers of names processed is available through a counter using the reporter object.
    }
}
