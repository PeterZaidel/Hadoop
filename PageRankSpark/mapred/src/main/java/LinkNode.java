import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class LinkNode implements Writable {

    public static String DELIMETER = "<::>";

    public String link = "";
    public double pr = 0.0;//1.0/ PageRankJob.N;


    public LinkNode()
    {}

    public LinkNode(String link)
    {
        this.link = link;
    }

    public LinkNode(String link, double pr)
    {
        this.link = link;
        this.pr = pr;
    }



    public void write(DataOutput out) throws IOException {
        out.writeUTF(link);
        out.writeDouble(pr);
    }

    public void readFields(DataInput in) throws IOException {
        link = in.readUTF();
        pr = in.readDouble();
    }

    public void parseString(String in)
    {
        String[] args = in.split(DELIMETER);
        link = args[0];
        pr = Double.parseDouble(args[1]);
    }

    public String getLink()
    {
        return link;
    }

    public double getPR() {
        return pr;
    }


    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(link);
        stringBuilder.append(DELIMETER);
        stringBuilder.append(pr);

        return stringBuilder.toString();

    }
}