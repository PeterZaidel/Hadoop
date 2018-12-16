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

public class CountNodesJob extends Configured implements Tool {

    public static final String UNIQ_LINKS_GROUP = "UNIQ_LINKS_GROUP";
    public static final String UNIQ_LINKS_COUNTER = "UNIQ_LINKS_COUNTER";
    public static final String END_LINKS_COUNTER = "END_LINKS_COUNTER";

    public static final String UNIQ_TAG = "<UNIQ>";
    public static final String TAG_DELIMETER = "<TAG>";
    public static final String OUT_TAG = "<OUT>";
    public static final String IN_TAG = "<IN>";

    public static final String DELIMETER = "\t";


    public static class CountNodesMapper extends Mapper<LongWritable, Text, Text, Text> {


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
                return;
            }

            String from = args[0];
            String to = args[1];

            context.write(new Text(from), new Text(OUT_TAG));
            context.write(new Text(to), new Text(IN_TAG));

        }
    }

    public static class CountNodesReducer extends Reducer<Text, Text, Text, Text>
    {
        @Override
        protected void reduce(Text url, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.getCounter(UNIQ_LINKS_GROUP, UNIQ_LINKS_COUNTER).increment(1);

            int out_count = 0;
            int in_count = 0;

            for(Text v : values)
            {
                if(v.toString().equals(OUT_TAG))
                {
                    out_count++;
                }

                if(v.toString().equals(IN_TAG))
                {
                    in_count++;
                }
            }

            if(out_count == 0)
            {
                context.getCounter(UNIQ_LINKS_GROUP, END_LINKS_COUNTER).increment(1);
                context.write(url, new Text("OUT: " + Integer.toString(out_count)
                        + "   IN: " + Integer.toString(in_count)  ));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1]);
        int res = job.waitForCompletion(true) ? 0 : 1;

        System.out.println("ALL_URLS: " + job.getCounters().findCounter(UNIQ_LINKS_GROUP, UNIQ_LINKS_COUNTER).getValue());
        System.out.println("END_URLS: " + job.getCounters().findCounter(UNIQ_LINKS_GROUP, END_LINKS_COUNTER).getValue());


        return res;
    }

    private static Job GetJobConf(Configuration conf, final String input, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(CountNodesJob.class);
        job.setJobName(CountNodesJob.class.getCanonicalName());

        job.setInputFormatClass(NLineInputFormat.class);

        NLineInputFormat.addInputPath(job, new Path(input));


        TextOutputFormat.setOutputPath(job, new Path(output));
        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 100000);

        job.setMapperClass(CountNodesMapper.class);
        job.setReducerClass(CountNodesReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        return job;
    }



    public static void main(String[] args) throws Exception {

        deleteDirectory(new File(args[1]));

        int exitCode = ToolRunner.run(new CountNodesJob(), args);
        System.exit(exitCode);
    }
}