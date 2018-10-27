import com.sun.jersey.core.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.print.DocFlavor;
import javax.xml.soap.Node;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.zip.DataFormatException;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.ZipException;




public class PageRankJob extends Configured implements Tool {

    public static double alpha = 0.1;
    public static long N = 4086514;
    public static int Iterations = 3;

    private static final String HEADER_NODE = "<<NODE>>";
    private static final String HEADER_REC = "<<REC>>";

    public static final String PR_GROUP = "PageRank_GROUP";
    public static final String PR_END_SUM = "PR_END_SUM";


    public static class CountEndNodesPRMapper extends Mapper<LongWritable, Text, Text, Text>{

        Text end_node_head = new Text("<END_NODE>");
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String input_text = value.toString();

            int split_index = input_text.indexOf("\t");
            String header_url = input_text.substring(0, split_index);
            input_text = input_text.substring(split_index+1);

            try {
                Record rec = new Record();
                rec.parseString(input_text);

                if(rec.out_nodes.size() == 0)
                {
                    context.write(end_node_head, new Text(rec.head.toString()));
                }
            }
            catch (Exception e)
            {
                return;
            }


        }
    }

    public static class CountEndNodesPRReducer  extends  Reducer<Text,Text, Text, Text>{
        @Override
        protected void reduce(Text head, Iterable<Text> nodes_text, Context context) throws IOException, InterruptedException {
            double sum_end_nodes_pr = 0;
            for(Text t : nodes_text)
            {
                LinkNode n = new LinkNode();
                n.parseString(t.toString());

                sum_end_nodes_pr += n.pr;
            }

            context.write(new Text(""), new Text(Double.toString(sum_end_nodes_pr)));
        }
    }




    public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String input_text = value.toString();

            int split_index = input_text.indexOf("\t");
            String header_url = input_text.substring(0, split_index);
            input_text = input_text.substring(split_index+1);

            Record rec = new Record();
            rec.parseString(input_text);

            context.write(new Text(header_url), new Text(HEADER_REC + rec.toString()));


            if(rec.out_nodes.size() == 0)
            {
//                double end_nodes_pr_sum = Double.longBitsToDouble(context.getCounter(PR_GROUP, PR_END_SUM).getValue());
//                end_nodes_pr_sum += rec.head.pr;
//                context.getCounter(PR_GROUP, PR_END_SUM).setValue(Double.doubleToLongBits(end_nodes_pr_sum));
                return;
            }

            double to_pr = rec.head.getPR()/rec.out_nodes.size();
            for(LinkNode n : rec.out_nodes)
            {
                if (n.getLink().length() == 0)
                {
                    continue;
                }

                LinkNode to = new LinkNode();
                to.link = rec.head.link;
                to.pr = to_pr;
//                n.pr  = to_pr;
//
//                Record node_rec = new Record();
//                node_rec.head = rec.head;

                context.write(new Text(n.getLink()), new Text(HEADER_NODE + to.toString()));
            }
        }
    }

    public static class PageRankReducer extends Reducer<Text,Text, Text, Text>
    {
        @Override
        protected void reduce(Text node_url, Iterable<Text> nodes_text, Context context) throws IOException, InterruptedException {

            Record rec = new Record(node_url.toString());
            double rank = 0;

            boolean loaded_rec = false;

            List<LinkNode> in_nodes = new LinkedList<>();
            for (Text tt : nodes_text) {
                String text_data = tt.toString();
                if(text_data.contains(HEADER_NODE))
                {
                    text_data = text_data.replace(HEADER_NODE, "");
                    LinkNode n = new LinkNode();
                    n.parseString(text_data);

                    in_nodes.add(n);

                    rank += n.pr;
                    continue;
                }

                if(text_data.contains(HEADER_REC))
                {
                    loaded_rec = true;
                    text_data = text_data.replace(HEADER_REC, "");
                    rec.parseString(text_data);
                }
            }

            if(!loaded_rec)
            {
                rec.in_nodes = in_nodes;
            }

            //double end_nodes_pr_sum = Double.longBitsToDouble(context.getCounter(PR_GROUP, PR_END_SUM).getValue());

            //rank = end_nodes_pr_sum/N + (alpha) / N + (1.0 - alpha) * rank;
            rank =  (alpha) / N + (1.0 - alpha) * rank;
            rec.head.pr = rank;

            context.write(new Text(node_url), new Text(rec.toString()));
        }
    }

    @Override
    public int run(String[] args) throws Exception
    {
//        N = Integer.parseInt(System.getProperty("N", "5"));
//        alpha = Double.parseDouble(System.getProperty("alpha", "0.1"));
//        Iterations = Integer.parseInt(System.getProperty("iter", "2"));
        System.out.println("PAGE RANK JOB!!!");


        int iterations = Iterations;//Integer.parseInt(args[0]);
        String input_file = args[0];
        String output = args[1];
        Configuration conf = getConf();

        String inputFormat  = "%s/it%02d/part-r-*";
        String outputFormat = "%s/it%02d/";
        String en_sumOutputFormat = "%s/it%02d_en_sum/";
        String en_sumNodesInputFormat = "%s/it%02d_en_sum/part-r-*";

        String inputStep = "", outputStep = "";
        String ensumInputStep = "",ensumOutputStep = "";

        Job[] steps = new Job[iterations];

        ensumOutputStep = String.format(en_sumOutputFormat, output, 1);
        Job enSumJob = GetCountEndNodesConf(conf, input_file, ensumOutputStep );
        if(!enSumJob.waitForCompletion(true))
        {
            System.out.println("Error in enSumJob! iter: 0");
            return 1;
        }

        outputStep = String.format(outputFormat, output, 1);
        steps[0] = GetJobConf(conf, input_file, outputStep, 1);
        if(!steps[0].waitForCompletion(true))
        {
            System.out.println("Error in PRJob! iter: 0");
            return 1;
        }



        for (int i = 1; i < iterations; i++) {
            inputStep  = String.format(inputFormat,  output, i);
            outputStep = String.format(outputFormat, output, i + 1);

            ensumInputStep = String.format(inputFormat,  output, i);
            ensumOutputStep = String.format(en_sumOutputFormat, output, i+1);

            enSumJob = GetCountEndNodesConf(conf, ensumInputStep, ensumOutputStep );
            if(!enSumJob.waitForCompletion(true))
            {
                System.out.println("Error in enSumJob! iter: " + Integer.toString(i));
                return 1;
            }

            steps[i] = GetJobConf(conf, inputStep, outputStep, i + 1);
            if(!steps[i].waitForCompletion(true))
            {
                System.out.println("Error in PRJob! iter: " + Integer.toString(i));
                return 1;
            }
        }
        return 0;
    }


    private static Job GetJobConf(Configuration conf, String input, String output, int currentIteration) throws IOException {
        Job job = Job.getInstance(conf);

//        job.getCounters().findCounter(PR_GROUP, PR_END_SUM).setValue(0);

        job.setJarByClass(PageRankJob.class);
        job.setJobName(PageRankJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

//        FileSystem fs = new Path("/").getFileSystem(conf);

        TextInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        return job;
    }


    private static Job GetCountEndNodesConf(Configuration conf, String input, String output) throws IOException {
        Job job = Job.getInstance(conf);

        job.setJarByClass(PageRankJob.class);
        job.setJobName(PageRankJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

        TextInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(CountEndNodesPRMapper.class);
        job.setReducerClass(CountEndNodesPRReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        return job;
    }
    static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

    public static void main(String[] args) throws Exception {

//        //TODO: TEST
        deleteDirectory(new File(args[1]));

        int exitCode = ToolRunner.run(new PageRankJob(), args);
        System.exit(exitCode);
    }
}