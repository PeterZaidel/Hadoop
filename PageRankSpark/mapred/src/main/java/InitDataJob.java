import com.sun.jersey.core.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.zip.DataFormatException;
import java.util.zip.Inflater;

import static org.apache.commons.io.FileUtils.deleteDirectory;

public class InitDataJob extends Configured implements Tool {


    public static final String DELIMETER = "\t";
    public static final String OUT_TAG = "<OUT>";
    public static final String IN_TAG = "<IN>";
    public static final String TAG_DELIMETER = "<TAG>";


    public static class InitDataMapper extends Mapper<LongWritable, Text, Text, Text> {


        private Text one = new Text("1");

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String text = value.toString();
            if(text.isEmpty() || text.contains("#"))
            {
                return;
            }

            String[] args = text.split(DELIMETER);
            if(args.length < 2)
            {
                throw  new InterruptedException("input args format error!!: " + text);
            }

            String from = args[0];
            String to = args[1];


            context.write(new Text(from), new Text(OUT_TAG + TAG_DELIMETER + to));
            context.write(new Text(to), new Text(IN_TAG + TAG_DELIMETER + from));

        }
    }

    public static class InitDataReducer extends Reducer<Text, Text, Text, Text>
    {

        double InitPR = 1.0/ Constants.N;

        @Override
        protected void reduce(Text url, Iterable<Text> values, Context context) throws IOException, InterruptedException {


            Record record = new Record();
            record.head = new LinkNode();

            record.head.pr = InitPR;
            record.head.link = url.toString();


            for (Text v: values)
            {
                String[] args = v.toString().split(TAG_DELIMETER);
                if(args[0].equals(OUT_TAG))
                {
                    LinkNode n = new LinkNode();
                    n.link = args[1];
                    n.pr = InitPR;
                    record.out_nodes.add(n);
                }

                if(args[0].equals(IN_TAG))
                {
                    LinkNode n = new LinkNode();
                    n.link = args[1];
                    n.pr = InitPR;
                    record.in_nodes.add(n);
                }
            }

            context.write(new Text(record.head.link), new Text(record.toString()));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1]);


        TimerTask task = new TimerTask() {
            public void run() {
                try {
                    System.out.println("map: " + job.mapProgress());
                    System.out.println("reduce: " + job.reduceProgress());
                } catch (Exception e) {
                    System.out.println("err");
                }
            }
        };
        Timer timer = new Timer("Timer");

        timer.scheduleAtFixedRate(task, 20000L, 5000L);

        int res = job.waitForCompletion(true) ? 0 : 1;

        timer.cancel();
        timer.purge();

        return res;
    }

    private static Job GetJobConf(Configuration conf, final String input, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(InitDataJob.class);
        job.setJobName(InitDataJob.class.getCanonicalName());

        job.setInputFormatClass(NLineInputFormat.class);

        NLineInputFormat.addInputPath(job, new Path(input));


        TextOutputFormat.setOutputPath(job, new Path(output));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 10000);

        job.setMapperClass(InitDataMapper.class);
        job.setReducerClass(InitDataReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(20);



        return job;
    }



    public static void main(String[] args) throws Exception {

        deleteDirectory(new File(args[1]));

        int exitCode = ToolRunner.run(new InitDataJob(), args);
        System.exit(exitCode);
    }
}