import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.BasicConfigurator;

import java.io.*;
import java.util.*;


public class PageRankJob extends Configured implements Tool {

    private static final String TAG_DELIMETER = "<TAG>";
    private static final String HEADER_NODE_TAG = "<NODE>";
    private static final String HEADER_REC_TAG = "<REC>";
    private static final String ACCUM_PR_TAG = "<ACCPR>";
    private static final String SUMPR_TAG = "<SUMPR_TAG>";

    public static final String PR_GROUP = "PageRank_GROUP";
    public static final String PR_END_NODES_COUNT = "PR_END_NODES_COUNT";
    public static final String ACCUMULATED_PR = "ACCUMULATED_PR";
    public static final String PR_SUM = "PR_SUM";

    public static final String ACC_PR_GROUP_NAME = "accumPr";
    public static final String PAGERANK_GROUP_NAME = "rank";
    public static final String SUM_PR_GROUP_NAME = "sumPr";

    public static final String ACC_PR_FILENAME_TAG = "<ACC_PR_FILENAME>";
    public static final String SUM_PR_FILENAME_TAG = "<SUM_PR_FILENAME>";

    public static final String CURRENT_ITER_TAG = "<CUR_ITER>";


    public static final String JOB_TYPE_TAG = "<JOB_TAG>";
    public static final String JOB_PR_ITER = "PRIT";
    public static final String JOB_ACCUM_PR = "ACCPR";


    public static final String STABILIZED_COUNT = "STAB";


    public static double ReadNumberFile(String filename, Configuration conf) throws IOException {
        double value = 0;
        boolean readed = false;

        FileSystem fs = FileSystem.get(conf);
        Path p = new Path(filename);
        FileStatus[] statuses = fs.globStatus(p);

        if(statuses.length == 0)
        {
            throw  new IOException("none files by expression!!");
        }

        for (FileStatus status : statuses) {
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
            String line;
            line = br.readLine();
            while (line != null) {
                line = line.split("\t")[1];
                double val = Double.parseDouble(line);
                value += val;
                line = br.readLine();
                readed = true;
            }
        }

        if(!readed)
        {
            throw  new IOException("none files by expression!!");
        }

        return value;
    }


    public static class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

        private String jobType = JOB_PR_ITER;

        public void setup(Mapper.Context context) throws IOException
        {
            jobType = context.getConfiguration().get(JOB_TYPE_TAG);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String input_text = value.toString();

            int split_index = input_text.indexOf("\t");
            String header_url = input_text.substring(0, split_index);
            input_text = input_text.substring(split_index+1);

            Record rec = new Record();
            rec.parseString(input_text);

            if(jobType.equals(JOB_ACCUM_PR) )
            {
                context.write(new Text(SUMPR_TAG),
                        new Text(Double.toString(rec.head.pr)));
                if(rec.out_nodes.size() == 0)
                {
                    context.write(new Text(ACCUM_PR_TAG), new Text(Double.toString(rec.head.pr)));
                }
                return;
            }


            context.write(new Text(header_url), new Text(HEADER_REC_TAG + TAG_DELIMETER
                    + rec.toString()));

            context.write(new Text(SUMPR_TAG),
                    new Text(Double.toString(rec.head.pr)));

            if(rec.out_nodes.size() == 0)
            {
                context.write(new Text(ACCUM_PR_TAG), new Text(Double.toString(rec.head.pr)));
                return;
            }



            long out_size = 0;
            for(LinkNode n: rec.out_nodes)
            {
                if(n.link.length() > 0)
                {
                    out_size++;
                }
            }

            if(out_size == 0)
            {
                return;
            }


            double to_pr = rec.head.pr/(double) (out_size);
            for(LinkNode n_to : rec.out_nodes)
            {
                if(n_to.link.length() == 0)
                {
                    continue;
                }

                LinkNode from = new LinkNode();
                from.link = rec.head.link;
                from.pr = to_pr;

                context.write(new Text(n_to.getLink()), new Text(HEADER_NODE_TAG + TAG_DELIMETER
                        + Double.toString(to_pr)));
            }
        }
    }

    public static class PageRankReducer extends Reducer<Text,Text, Text, Text>
    {
        private MultipleOutputs mos;

        double accumulatedPr = 0.0;

        public void setup(Reducer.Context context) throws IOException
        {
            mos = new MultipleOutputs(context);

            String accumFileName = context.getConfiguration().get(ACC_PR_FILENAME_TAG);
            if(accumFileName != null) {
                accumulatedPr = ReadNumberFile(accumFileName, context.getConfiguration());
            }
        }

        private void processAccumPr(Text nodeRecord, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double pr = 0;
            for (Text v : values)
            {
                pr += Double.parseDouble(v.toString());
            }

            String filename = ACC_PR_GROUP_NAME + "/part";

            mos.write(ACC_PR_GROUP_NAME, new Text("accPR"),
                    new Text(Double.toString(pr)),
                    filename
                    );
        }


        private void processSumPr(Text nodeRecord, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double pr = 0;
            for (Text v : values)
            {
                pr += Double.parseDouble(v.toString());
            }

            String filename = SUM_PR_GROUP_NAME + "/part";

            mos.write(SUM_PR_GROUP_NAME, new Text("sumPR"),
                    new Text(Double.toString(pr)),
                    filename
            );
        }




        @Override
        protected void reduce(Text nodeRecord, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            if (nodeRecord.toString().equals(ACCUM_PR_TAG))
            {
                processAccumPr(nodeRecord, values, context);
                return;
            }

            if (nodeRecord.toString().equals(SUMPR_TAG))
            {
                processSumPr(nodeRecord, values, context);
                return;
            }

            Record rec = new Record(nodeRecord.toString());
            double rank = 0;

            boolean loaded_rec = false;

            List<LinkNode> in_nodes = new LinkedList<>();
            List<LinkNode> out_nodes = new LinkedList<>();
            for (Text v : values)
            {
                String[] args = v.toString().split(TAG_DELIMETER);
                if(args[0].equals(HEADER_NODE_TAG))
                {
                    double pr = Double.parseDouble(args[1]);
                    rank += pr;
                }

                if(args[0].equals(HEADER_REC_TAG))
                {
                    loaded_rec = true;
                    rec.parseString(args[1]);
                }
            }



            rank = Constants.alpha * 1.0/ (double) Constants.N + (1.0 - Constants.alpha) * (rank + accumulatedPr/ (double) Constants.N);
            if(Math.abs(rank - rec.head.pr) < 1E-4)
            {
                context.getCounter(PR_GROUP, STABILIZED_COUNT).increment(1);
            }

            rec.head.pr =  rank;

            context.write(new Text(nodeRecord), new Text(rec.toString()));

        }

        public void cleanup(Reducer.Context context) throws IOException, InterruptedException {
            mos.close();
        }
    }

    @Override
    public int run(String[] args) throws Exception
    {
        System.out.println("PAGE RANK JOB!!!");
        System.out.println("Iterations: " + Integer.toString(Constants.Iterations));

        String input_file = args[0];
        String output = args[1];
        Configuration conf = getConf();
        FileSystem fs = FileSystem.get(new Configuration());

        String inputFormat  = "%s/it%02d/part-r-*";
        String sumPrFormat = "%s/it%02d/" + SUM_PR_GROUP_NAME + "/*";
        String accumPrFormat = "%s/it%02d/" + ACC_PR_GROUP_NAME + "/*";
        String outputFormat = "%s/it%02d/";
        String inputStep = "", outputStep = "";
        String sumPrStep ="", accumPrStep = "";

        ArrayList<Job> steps = new ArrayList<>();

        {
            System.out.println("PageRank iter: 0 initialization accumulated PR");
            outputStep = String.format(outputFormat, output, 0);
            Job initJob = GetJobConf(conf, input_file, outputStep, 0);
            initJob.getConfiguration().set(CURRENT_ITER_TAG, Integer.toString(0));
            initJob.getConfiguration().set(JOB_TYPE_TAG, JOB_ACCUM_PR);

            TimerTask task = new TimerTask() {
                public void run() {
                    try {
                        System.out.println("map: " + initJob.mapProgress());
                        System.out.println("reduce: " + initJob.reduceProgress());
                    } catch (Exception e) {
                        System.out.println("err");
                    }
                }
            };
            Timer timer = new Timer("Timer");

            timer.scheduleAtFixedRate(task, 20000L, 5000L);


            if(!initJob.waitForCompletion(true))
            {
                System.out.println("Error in PRJob! iter: 0 initialization");
                return 1;
            }

            timer.cancel();
            timer.purge();

            accumPrStep = String.format(accumPrFormat, output, 0);
            sumPrStep = String.format(sumPrFormat, output, 0);

            double accumPr = ReadNumberFile(accumPrStep, conf);
            double sumPr = ReadNumberFile(sumPrStep, conf);

            System.out.println("--ACCUM PR: " + accumPr);
            System.out.println("--SUM PR: " + sumPr);
        }


        System.out.println("PageRank iter: 1");
        outputStep = String.format(outputFormat, output, 1);
        Job prJob = GetJobConf(conf, input_file, outputStep, 1);
        prJob.getConfiguration().set(CURRENT_ITER_TAG, Integer.toString(1));
        prJob.getConfiguration().set(ACC_PR_FILENAME_TAG, accumPrStep);
        prJob.getConfiguration().set(SUM_PR_FILENAME_TAG, sumPrStep);
        prJob.getConfiguration().set(JOB_TYPE_TAG, JOB_PR_ITER);



        steps.add(prJob);
        {
            TimerTask task = new TimerTask() {
                public void run() {
                    try {
                        System.out.println("map: " + steps.get(steps.size() - 1).mapProgress());
                        System.out.println("reduce: " + steps.get(steps.size() - 1).reduceProgress());
                    } catch (Exception e) {
                        System.out.println("err");
                    }
                }
            };
            Timer timer = new Timer("Timer");

            timer.scheduleAtFixedRate(task, 20000L, 5000L);

            if (!steps.get(0).waitForCompletion(true)) {
                System.out.println("Error in PRJob! iter: 1");
                return 1;
            }

            timer.cancel();
            timer.purge();
        }

        double stabilizedPercent = steps.get(0).getCounters().findCounter(PR_GROUP, "STAB").getValue()/ (double)(Constants.N);
        System.out.println("STABILIZATION: " + stabilizedPercent);
        System.out.println("SET_DEF_PR: " + steps.get(0).getCounters().findCounter(PR_GROUP, "DEFAULT_PR").getValue());

        accumPrStep = String.format(accumPrFormat, output, 1);
        sumPrStep = String.format(sumPrFormat, output, 1);

        double accumPr = ReadNumberFile(accumPrStep, conf);
        double sumPr = ReadNumberFile(sumPrStep, conf);

        System.out.println("--ACCUM PR: " + accumPr);
        System.out.println("--SUM PR: " + sumPr);



        for (int i = 1; i <Constants.Iterations; i++) {

            System.out.println("PageRank iter: " + Integer.toString(i));
            inputStep  = String.format(inputFormat,  output, i);

            outputStep = String.format(outputFormat, output, i + 1);


            prJob = GetJobConf(conf, inputStep, outputStep, i + 1);

            prJob.getConfiguration().set(CURRENT_ITER_TAG, Integer.toString(i+1));
            prJob.getConfiguration().set(ACC_PR_FILENAME_TAG, accumPrStep);
            prJob.getConfiguration().set(SUM_PR_FILENAME_TAG, sumPrStep);
            prJob.getConfiguration().set(JOB_TYPE_TAG, JOB_PR_ITER);

            steps.add(prJob);

            {
                TimerTask task = new TimerTask() {
                    public void run() {
                        try {
                            System.out.println("map: " + steps.get(steps.size() - 1).mapProgress());
                            System.out.println("reduce: " + steps.get(steps.size() - 1).reduceProgress());
                        } catch (Exception e) {
                            System.out.println("err");
                        }
                    }
                };
                Timer timer = new Timer("Timer");

                timer.scheduleAtFixedRate(task, 20000L, 5000L);
                if (!steps.get(i).waitForCompletion(true)) {
                    System.out.println("Error in PRJob! iter: " + Integer.toString(i));
                    return 1;
                }
                timer.cancel();
                timer.purge();
            }

            stabilizedPercent = steps.get(i).getCounters().findCounter(PR_GROUP, STABILIZED_COUNT).getValue() / (double)(Constants.N);
            System.out.println("--STABILIZATION: " + stabilizedPercent);
            System.out.println("--SET_DEF_PR: " + steps.get(i).getCounters().findCounter(PR_GROUP, "DEFAULT_PR").getValue());

            accumPrStep = String.format(accumPrFormat, output, i+1);
            sumPrStep = String.format(sumPrFormat, output, i+1);

            accumPr = ReadNumberFile(accumPrStep, conf);
            sumPr = ReadNumberFile(sumPrStep, conf);

            System.out.println("--ACCUM PR: " + accumPr);
            System.out.println("--SUM PR: " + sumPr);

            System.out.println("--Stabilized percent: " + stabilizedPercent);
        }


        return 0;
    }


    private static Job GetJobConf(Configuration conf, String input, String output, int currentIteration) throws IOException {
        Job job = Job.getInstance(conf);


        job.setJarByClass(PageRankJob.class);
        job.setJobName(PageRankJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(output));

        MultipleOutputs.addNamedOutput(job, ACC_PR_GROUP_NAME, TextOutputFormat.class,
                Text.class, Text.class);

        MultipleOutputs.addNamedOutput(job, SUM_PR_GROUP_NAME, TextOutputFormat.class,
                Text.class, Text.class);
        MultipleOutputs.addNamedOutput(job, PAGERANK_GROUP_NAME, TextOutputFormat.class,
                Text.class, Text.class);

        TextInputFormat.addInputPath(job, new Path(input));

        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(Constants.Reducers);

        return job;
    }



    public static void main(String[] args) throws Exception {

        System.out.println("PageRank Started! ");

        Utils.deleteDirectory(new File(args[1]));

        int exitCode = ToolRunner.run(new PageRankJob(), args);
        System.exit(exitCode);
    }
}