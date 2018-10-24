import com.sun.xml.internal.bind.v2.runtime.unmarshaller.XsiNilLoader;
import org.apache.hadoop.util.ToolRunner;

import javax.xml.soap.Node;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TestPageRank
{

    public static void createTestLinkGraphFile(String foldername) throws FileNotFoundException, UnsupportedEncodingException {
        PrintWriter writer = new PrintWriter(foldername + "part-r-00000", "UTF-8");

        NodeWritable nodeA = new NodeWritable("A", 0.0, Arrays.asList("B", "D"));
        NodeWritable nodeB = new NodeWritable("B", 0.0, Arrays.asList("E"));
        NodeWritable nodeC = new NodeWritable("C", 0.0, Arrays.asList("A"));
        NodeWritable nodeD = new NodeWritable("D", 0.0, Arrays.asList("A"));
        NodeWritable nodeE = new NodeWritable("E", 0.0, Arrays.asList("A"));

        List<NodeWritable> nodes = Arrays.asList(nodeA, nodeB, nodeC,
                                                 nodeD, nodeE);

        for(NodeWritable n: nodes)
        {
            writer.println(n.getNodeUrl() + "\t" + n.toString());
        }

        writer.close();
    }

    static void TestNodeWriter()
    {
        NodeWritable node1 = new NodeWritable("http://lenta.ru/news/2006/05/04/koshtinya/",
                10.00, Arrays.asList("http://www.facebook.com/2008/fbml",
                                          "http://lenta.ru/news/2014/10/27/internetpackage/"));

        String str1 = node1.toString();

        NodeWritable node2 = new NodeWritable();
        node2.parseString(str1);

        boolean res = true;

        assert (node1.getNodeUrl().equals(node2.getNodeUrl()));
        assert (node1.getRank() == node2.getRank());
        assert (node1.getLinksSize() == node2.getLinksSize());
        for(int i = 0; i < node1.getLinksSize(); i++ )
        {
            assert node1.getLinks().get(i).equals(node2.getLinks().get(i));
        }
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

        TestNodeWriter();

        deleteDirectory(new File(args[1]));



        createTestLinkGraphFile(args[0]);

        PageRankJob.N = 5;
        PageRankJob.Iterations = 2;
        PageRankJob.alpha = 0.1;
        args[0] = args[0]+"part-r-*";
        int exitCode = ToolRunner.run(new PageRankJob(), args);
        System.exit(exitCode);

    }
}
