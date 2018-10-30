import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

// На вход - id+base64(gzip(html))
// На выход - id->ids from urls.txt
public class GetClicksJob extends Configured implements Tool {
    static final LongWritable one = new LongWritable(1);

    public static class ClicksMapper extends Mapper<Text, Text, TextTextLong, LongWritable> {
        @Override
        protected void map(Text query, Text urlText, Context context) throws IOException, InterruptedException {
            String urlLine = urlText.toString();
            URL url;
            try {
                url = new URL(urlLine);
            }
            catch (Exception ex){
                System.out.println(urlLine);
                return;
            }
            String host = url.getHost();
            TextTextLong compositeKey = new TextTextLong(new Text(host), new Text(query), one);
            context.write(compositeKey, one);
        }
    }


    public static class ClicksReducer extends Reducer<TextTextLong, LongWritable, TextTextLong, LongWritable> {
        @Override
        protected void reduce(TextTextLong key, Iterable<LongWritable> counts, Context context) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable i:counts){
                sum += i.get();
            }

            context.write(new TextTextLong(key.getFirst(),key.getSecond(), new LongWritable(sum)) , one);
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(GetClicksJob.class);
        job.setJobName(GetClicksJob.class.getCanonicalName());
        // will use traditional TextInputFormat to split line-by-line
        //TextInputFormat.addInputPath(job, new Path(input));
        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.setMapperClass(ClicksMapper.class);
        job.setReducerClass(ClicksReducer.class);
        job.setNumReduceTasks(5);
        job.setMapOutputKeyClass(TextTextLong.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(TextTextLong.class);
        job.setOutputValueClass(LongWritable.class);
        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new GetClicksJob(), args);
        System.exit(ret);
    }
}
