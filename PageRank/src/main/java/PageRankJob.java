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
    public static long N = 100000;
    public static int Iterations = 2;



    public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String header_url ="";
            String input_text = value.toString();

            int split_index = input_text.indexOf("\t");
            header_url = input_text.substring(0, split_index);
            input_text = input_text.substring(split_index+1);

            NodeWritable node = new NodeWritable();
            node.parseString(input_text);

            context.write(new Text(node.getNodeUrl()), new Text(node.toString()));

            if(node.getLinksSize() == 0)
            {
                return;
            }

            double link_rank = node.getRank()/node.getLinksSize();
            for(String link : node.getLinks())
            {
                if (link.length() == 0)
                {
                    continue;
                }

                NodeWritable cur_node = new NodeWritable(link, link_rank);
                context.write(new Text(link), new Text(cur_node.toString()));
            }




//
//            String[] args = value.toString().trim().split("\t");
//            String node_url = args[0];
//            double node_rank = Double.parseDouble(args[1]);
//
//            List<String> node_links = new ArrayList<>(Arrays.asList(args).subList(3, args.length));
//
//            context.write(new Text(node_url), new NodeWritable(node_rank, node_links));
//
//            if(node_links.size() == 0)
//            {
//                return;
//            }
//
//            double link_rank = node_rank / node_links.size();
//            for(String link : node_links)
//            {
//                context.write(new Text(link), new NodeWritable(link_rank));
//            }
        }
    }

    public static class PageRankReducer extends Reducer<Text,Text, Text, Text>
    {
        @Override
        protected void reduce(Text node_url, Iterable<Text> nodes_text, Context context) throws IOException, InterruptedException {
            double rank = 0;
            List<String> node_links = new ArrayList<>();

            List<String> input_strs = new ArrayList<>();

            for (Text node_str : nodes_text) {

                input_strs.add(node_str.toString());

                NodeWritable node = new NodeWritable();
                node.parseString(node_str.toString());
                if (node.getLinksSize() > 0) {
                    node_links.addAll(node.getLinks());
                } else {
                    rank += node.getRank();
                }
            }

            rank =  (alpha) / N + (1.0 - alpha) * rank;
            NodeWritable node = new NodeWritable(node_url.toString(),rank, node_links);
            context.write(new Text(node_url), new Text(node.toString()));
        }
    }

    @Override
    public int run(String[] args) throws Exception
    {
        N = Integer.parseInt(System.getProperty("N"));
        alpha = Double.parseDouble(System.getProperty("alpha", "0.1"));
        Iterations = Integer.parseInt(System.getProperty("iter", "5"));

//        Job job =  GetJobConf(getConf(), args[0], args[1], 1);
////        if (System.getProperty("mapreduce.input.indexedgz.bytespermap") != null) {
////            throw new Exception("Property = " + System.getProperty("mapreduce.input.indexedgz.bytespermap"));
////        }
//        return job.waitForCompletion(true) ? 0 : 1;

        int iterations = Iterations;//Integer.parseInt(args[0]);
        String input_file = args[0];
        String output = args[1];
        Configuration conf = getConf();

        String inputFormat  = "%s/it%02d/part-r-*";
        String outputFormat = "%s/it%02d/";

        String inputStep = "", outputStep = "";
        Job[] steps = new Job[iterations];

        outputStep = String.format(outputFormat, output, 1);
        steps[0] = GetJobConf(conf, input_file, outputStep, 1);
        boolean job_res = steps[0].waitForCompletion(true);
        if(!job_res)
        {
            return 1;
        }

        for (int i = 1; i < iterations; i++) {
            inputStep  = String.format(inputFormat,  output, i);
            outputStep = String.format(outputFormat, output, i + 1);
            steps[i] = GetJobConf(conf, inputStep, outputStep, i + 1);
            job_res = steps[i].waitForCompletion(true);
            if(!job_res)
            {
                return 1;
            }
        }
        return 0;
    }


    private static Job GetJobConf(Configuration conf, String input, String output, int currentIteration) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(PageRankJob.class);
        job.setJobName(PageRankJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));

        FileSystem fs = new Path("/").getFileSystem(conf);

        TextInputFormat.addInputPath(job, new Path(input));

//        RemoteIterator<LocatedFileStatus> fileListItr = fs.listFiles(new Path(input), false);
//
//        while (fileListItr != null && fileListItr.hasNext()) {
//            LocatedFileStatus file = fileListItr.next();
//            if (file.toString().contains("part")) {
//                TextInputFormat.addInputPath(job, file.getPath());
//            }
//        }

        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);


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

        //TODO: TEST
        deleteDirectory(new File(args[1]));

        int exitCode = ToolRunner.run(new PageRankJob(), args);
        System.exit(exitCode);
    }
}