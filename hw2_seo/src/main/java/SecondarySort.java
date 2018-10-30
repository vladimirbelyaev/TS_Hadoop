import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;


public class SecondarySort extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int rc = ToolRunner.run(new SecondarySort(), args);
        System.exit(rc);
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }


    public static class ClickRecordPartitioner extends Partitioner<TextTextLong, LongWritable> { // По хосту (first)
        @Override
        public int getPartition(TextTextLong key, LongWritable val, int numPartitions) {
            // Все запросы, приведшие к одному хосту - на один reducer
            return Math.abs(key.getFirst().hashCode()) % numPartitions;
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(TextTextLong.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return ((TextTextLong)a).compareTo((TextTextLong) b);
        }
    }

    public static class ClickRecordGrouper extends WritableComparator { // Группируем по хосту
        protected ClickRecordGrouper() {
            super(TextTextLong.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            Text a_first = ((TextTextLong)a).getFirst();
            Text b_first = ((TextTextLong)b).getFirst();
            return a_first.compareTo(b_first);
        }
    }

    public static class ClickRecordMapper extends Mapper<Text, Text, TextTextLong, LongWritable> {
        private long minclicks;
        @Override
        protected void setup(Context context){
            minclicks = context.getConfiguration().getLong("seo.minclicks", 0);
            System.out.println("Parameter seo.minclicks = " + minclicks);
        }
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            TextTextLong TTLkey = new TextTextLong(key.toString());
            LongWritable freq = new LongWritable(Long.parseLong(value.toString()));
            if (TTLkey.getThird().get() >= minclicks){
                context.write(TTLkey, freq);
            }
        }
    }

    public static class ClickRecordReducer extends Reducer<TextTextLong, LongWritable, Text, Text> {

        @Override
        protected void reduce(TextTextLong key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key.getFirst(), new Text(key.getSecond().toString() + "\t" + key.getThird().toString()));

        }
    }

    Job GetJobConf(Configuration conf, String input, String out_dir) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(SecondarySort.class);
        job.setJobName(SecondarySort.class.getCanonicalName());

        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(out_dir));

        job.setNumReduceTasks(1);
        job.setMapperClass(ClickRecordMapper.class);
        job.setReducerClass(ClickRecordReducer.class);
        job.setPartitionerClass(ClickRecordPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(ClickRecordGrouper.class);

        job.setMapOutputKeyClass(TextTextLong.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        return job;
    }
}
