import com.sun.jersey.core.util.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.print.DocFlavor;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.HashMap;
import java.util.Map;
import java.util.zip.DataFormatException;
import java.util.zip.GZIPInputStream;
import java.util.zip.Inflater;
import java.util.zip.ZipException;

public class LinkGraphJob extends Configured implements Tool {

    public static class LinkGraphMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        static final IntWritable one = new IntWritable(1);

        static final String Header = "<HEAD>";

        // Pattern for recognizing a URL, based off RFC 3986
        private static final Pattern urlPattern = Pattern.compile(
                "(?:^|[\\W])((ht|f)tp(s?):\\/\\/|www\\.)"
                        + "(([\\w\\-]+\\.){1,}?([\\w\\-.~]+\\/?)*"
                        + "[\\p{Alnum}.,%_=?&#\\-+()\\[\\]\\*$~@!:/{};']*)",
                Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);


        private String decompress(String base64) throws IOException, DataFormatException {
            String encoded = base64;
            byte[] compressed = Base64.decode(encoded);

            try {
                // Decompress the bytes
                Inflater decompresser = new Inflater();
                decompresser.setInput(compressed, 0, compressed.length);

                byte[] result = new byte[compressed.length];
                StringBuilder outputString = new StringBuilder();
                int resultLength = decompresser.inflate(result);
                while (resultLength  > 0)
                {
                    outputString.append(new String(result, 0, resultLength, "UTF-8"));
                    resultLength = decompresser.inflate(result);
                }

                decompresser.end();

                // Decode the bytes into a String

                return outputString.toString();
            }
            catch (DataFormatException e)
            {
                return "";
            }

        }

        private List<String> find_urls(String text){
            List<String> res = new ArrayList<>();
            Matcher matcher = urlPattern.matcher(text);
            while (matcher.find()) {
                int matchStart = matcher.start(1);
                int matchEnd = matcher.end();
                // now you have the offsets of a URL match

                String url_text = text.substring(matchStart, matchEnd);
                res.add(url_text);
            }
            return res;
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
             String[] splitted_data = value.toString().split("\t");
             int doc_id = Integer.decode(splitted_data[0]);
             String raw_text = splitted_data[1];

             if (!Base64.isBase64(raw_text))
             {
                 // if string is not base64, it is string from urls.txt
                 // find url and add Header to it
                 List<String> urls = find_urls(raw_text);
                 context.write(new LongWritable(doc_id), new Text(Header + urls.get(0)));
                 return;
             }


            String html_text = null;
            try {
                html_text = decompress(raw_text);
                for(String url : find_urls(html_text))
                {
                    context.write(new LongWritable(doc_id), new Text(url));
                }
            } catch (DataFormatException e) {
                return;
            }


        }
    }

    public static class LinkGraphReducer extends Reducer<LongWritable, Text, Text, NodeWritable>
    {
        static final String Header = "<HEAD>";

        @Override
        protected void reduce(LongWritable url_idx, Iterable<Text> links, Context context) throws IOException, InterruptedException {


            List<String> node_links = new ArrayList<>();

            String header_link = "";
            for(Text link: links)
            {
                String link_str = link.toString();
                if(link_str.contains(Header)) {
                    header_link = link_str.replaceAll(Header, "");
                }
                else {

                    node_links.add(link_str);
                }
            }

            //TODO: TEST
            if (node_links.size() > 5) {
                node_links = node_links.subList(0, 5);
            }

            //StringBuilder output = new StringBuilder();

            double rank = 0.0;

//
//            String header_link = "";
//            for(Text link: links)
//            {
//                String link_str = link.toString();
//                if(link_str.contains(Header)) {
//                    header_link = link_str.replaceAll(Header, "");
//                }
//                else {
//
//                    output.append(link.toString()).append("\t");
//                }
//            }
//
//
//            String res = "";
////            res += url_idx.toString() + "\t";
//            res += rank.toString() + "\t";
////            res += header_link + "\t";
//            res += output.toString();
//            res += "\n";

            context.write(new Text(header_link), new NodeWritable(header_link, rank, node_links));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = GetJobConf(getConf(), args[0], args[1]);
//        if (System.getProperty("mapreduce.input.indexedgz.bytespermap") != null) {
//            throw new Exception("Property = " + System.getProperty("mapreduce.input.indexedgz.bytespermap"));
//        }
        return job.waitForCompletion(true) ? 0 : 1;
    }

    private static Job GetJobConf(Configuration conf, final String input, String output) throws IOException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(LinkGraphJob.class);
        job.setJobName(LinkGraphJob.class.getCanonicalName());

        job.setInputFormatClass(TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
//        TextInputFormat.addInputPath(job, new Path(input));

        FileSystem fs = new Path("/").getFileSystem(conf);

        RemoteIterator<LocatedFileStatus> fileListItr = fs.listFiles(new Path(input), false);

        while (fileListItr != null && fileListItr.hasNext()) {
            LocatedFileStatus file = fileListItr.next();
            if (file.toString().contains("txt")) {
                TextInputFormat.addInputPath(job, file.getPath());
            }
        }

        job.setMapperClass(LinkGraphMapper.class);
//        job.setCombinerClass(LinkGraphReducer.class);
        job.setReducerClass(LinkGraphReducer.class);

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

        int exitCode = ToolRunner.run(new LinkGraphJob(), args);
        System.exit(exitCode);
    }
}