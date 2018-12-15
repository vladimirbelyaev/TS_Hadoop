import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.*;

/* Логика работы:
Файлы с весами отдельно, структура отдельно.
Число сайтов берем из файла.
Первое заполнение весов надо отдельно закодить(можно этого и не делать, получим PageRank * n_links.
Random jump: знаем N, пересчитываем в Reducer.
Веса висячих вершин: кидаем в отдельные файлы, потом суммируем, в Mapper'е делаем добавку.
 */
public class PageRankJob extends Configured implements Tool {
    public static class PageRankMapper extends Mapper<Text, Text, Text, Text> {
        double alpha = 0.01;
        long N = 4847571;
        final Text hangingLink = new Text("HANGING_LINK");
        double avgLeak = 0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException, NullPointerException{
            FileSystem fs = ((FileSplit) context.getInputSplit()).getPath().getFileSystem(context.getConfiguration());
            String parentName = ((FileSplit) context.getInputSplit()).getPath().getParent().getName();
            Path dirPath = new Path("hw_4pr_mapred/" + parentName);
            FileStatus dirStat = fs.getFileStatus(dirPath);
            if (!dirStat.isDirectory()){
                throw new InterruptedException(dirStat.toString() + " is not a directory");
            }
            Path leakFile = new Path(" ");
            for (FileStatus fStatus :fs.listStatus(dirPath)){
                System.out.println(fStatus.toString());
                if (fStatus.getPath().toString().contains("leak")){
                    leakFile = new Path(fStatus.getPath().toString());
                }
            }
            if (leakFile.toString().equals(" ")){
                throw new InterruptedException("Leakfile is null");
            }

            fs = leakFile.getFileSystem(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(leakFile)));
            String curLine = br.readLine().trim();
            double leakedPR =  Double.parseDouble(curLine.split("\t")[1]);
            avgLeak = leakedPR/N;
        }
        @Override
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {


            String[] pair = value.toString().split("::::");
            if (pair.length == 1){
                // Висячая
                Text w = new Text(Double.toString( Double.parseDouble(pair[0]) + avgLeak * (1 - alpha)));
                context.write(hangingLink, w);
            } else {
                context.write(key, new Text("s" + pair[1]));
                String[] targets = pair[1].split(" ");
                Text w = new Text(Double.toString((Double.parseDouble(pair[0]) + avgLeak * (1 - alpha))/targets.length));
                for (String target:targets){
                    //System.out.println(target + "\t" + w.toString());
                    context.write(new Text(target), w);
                }
            }
        }
    }


    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        double alpha = 0.01;
        long N = 4847571;
        final Text hangingLink = new Text("HANGING_LINK");
        private MultipleOutputs<Text, Text> out;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException{
            // Ставим multiple output
            out = new MultipleOutputs<>(context);
        }
        @Override
        protected void reduce(Text key, Iterable<Text> data, Context context) throws IOException, InterruptedException {
            if (key.toString().equals(hangingLink.toString())){
                Double w = 0.0;
                for (Text i: data){
                    w += Double.parseDouble(i.toString());
                }
                out.write("leak", key, new Text(Double.toString(w)), "leak");
            }
            else{
                boolean hasStruct = false;
                double w = 0.0;
                String s = "";
                for (Text i: data){
                    if (i.toString().substring(0,1).equals("s")){
                        hasStruct = true;
                        s = i.toString().substring(1);
                    } else{
                        w += Double.parseDouble(i.toString());
                    }
                }
                w = w * (1 - alpha) + alpha * 1.0 / N;
                //System.out.println(key.toString() + "\t" + Double.toString(w) + "\t" + s);
                if (hasStruct){
                    context.write(key, new Text(Double.toString(w) + "::::" + s));
                } else {
                    context.write(key, new Text(Double.toString(w)));
                }
            }

        }
        @Override
        protected void cleanup(Context context
        ) throws IOException, InterruptedException {
            super.cleanup(context);
            out.close();
        }
    }

    private Job getJobConf(String input, String output) throws IOException {
        Job job = Job.getInstance(getConf());
        job.setJarByClass(PageRankJob.class);
        job.setJobName(PageRankJob.class.getCanonicalName());

        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        MultipleOutputs.addNamedOutput(job, "leak", TextOutputFormat.class,
                Text.class, Text.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(10);

        return job;
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = getJobConf(args[0], args[1]);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    static public void main(String[] args) throws Exception {
        //BasicConfigurator.configure();
        String dirName = args[0]; // hw_pagerank/iter_
        int nIter = Integer.parseInt(args[1]);
        String[] modifiedArgs = new String[2];
        int ret = 0;
        for (int i = 0; i < nIter - 1; i++){
            modifiedArgs[0] = dirName + Integer.toString(i) + "/part-*";
            modifiedArgs[1] = dirName + Integer.toString(i + 1);
            System.out.println("Starting iteration " + Integer.toString(i));
            ToolRunner.run(new PageRankJob(), modifiedArgs);
        }
        modifiedArgs[0] = dirName + Integer.toString(nIter - 1) + "/part-*";
        modifiedArgs[1] = dirName + "fin";
        ret = ToolRunner.run(new PageRankJob(), modifiedArgs);
        System.exit(ret);
    }
}
