import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Record implements Writable
{

    private static String DELIMETER = "\t";

    private LinkNode head;
    private List<LinkNode> out_nodes = new ArrayList<>();

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(head.toString());

        out.writeInt(out_nodes.size());
        for(LinkNode n : out_nodes)
        {
            out.writeUTF(n.toString());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        head = new LinkNode();
        head.parseString(in.readUTF());

        int out_size = in.readInt();
        for(int i =0; i< out_size; i++)
        {
            LinkNode n = new LinkNode();
            n.parseString(in.readUTF());
            out_nodes.add(n);
        }
    }

    public void parseString(String in)
    {
        String[] args = in.split(DELIMETER);
        head.parseString(args[0]);

        for(int i = 1; i < args.length; i++)
        {
            LinkNode n = new LinkNode();
            n.parseString(args[i]);
            out_nodes.add(n);
        }
    }


    public String toString()
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(head.toString());
        stringBuilder.append(DELIMETER);

        for(LinkNode n : out_nodes)
        {
            stringBuilder.append(n.toString());
            stringBuilder.append(DELIMETER);
        }
        return stringBuilder.toString();
    }
}
