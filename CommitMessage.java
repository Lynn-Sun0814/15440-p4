/**
 * CommitMessage class that serilize and deserilize messages for PL to send 
 * and receive
 */
import java.util.*;
import java.io.*;

public class CommitMessage implements Serializable {
    public String type;
    public String addr;
    public List<String> srcFilename;
    public byte[] img;
    public String filename;

    /**
     * CommitMessage constructor
     * @param String type
     * @param String id
     * @param Lisg<String> srcFilename
     * @param byte[] img
     * @param String filename
     */
    CommitMessage(String type, String id, List<String> srcFilename, byte[] img, String filename) {
        this.type = type;
        this.addr = id;
        this.srcFilename = srcFilename;
        this.img = img;
        this.filename = filename;
    }

    /**
     * serialize a commit message
     * @param CommitMessage msg
     * @return byte[]
     */
    public static byte[] makeMessage(CommitMessage msg) throws IOException {
        ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(byteOut);
        out.writeObject(msg);
        out.flush();
        out.close();
        return byteOut.toByteArray();
    }

    /**
     * deseralize a commit message
     * @param ProjectLib.Message msg
     * @return CommitMessage
     */
    public static CommitMessage parseMessage(ProjectLib.Message msg) throws IOException, ClassNotFoundException{
        ByteArrayInputStream byteIn = new ByteArrayInputStream(msg.body);
        ObjectInputStream in = new ObjectInputStream(byteIn);
        CommitMessage tmp = (CommitMessage) in.readObject();
        in.close();
        return tmp;
    }
}
