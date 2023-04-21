/**
 * Additional API layer for the server to receive messages for different 
 * collages
 */
import java.util.*;
import java.io.*;
import java.util.concurrent.*;

public class ReceiveMessage implements ProjectLib.MessageHandling{
    // maps the response message from a UN to the specific file commit queue
    public static ConcurrentHashMap<String, LinkedBlockingQueue<ProjectLib.Message>> commitMap = new ConcurrentHashMap<String, LinkedBlockingQueue<ProjectLib.Message>>();
    public static ConcurrentHashMap<String, LinkedBlockingQueue<ProjectLib.Message>> ACKMap = new ConcurrentHashMap<String, LinkedBlockingQueue<ProjectLib.Message>>();

    /** 
     * callback function that parse messages from the message queue and put
     * them to corresponding filename
     * @param ProjectLib.Message msg
     * @return boolean
     */
    public boolean deliverMessage( ProjectLib.Message msg ) {
        try{
            CommitMessage commitMsg = CommitMessage.parseMessage(msg);
            String filename = commitMsg.filename;
            String type = commitMsg.type;
            if (commitMap.containsKey(filename)) {
                commitMap.get(filename).add(msg);
            }
            else return false;
        } catch (Exception e) {
            System.err.println("Exception! " + e.getMessage());
            return false;
        }
        return true;
    }

    /**
     * add the filename to the map if there is a new commit
     * @param String filename
     */
    public void newCommit(String filename) {
        commitMap.put(filename, new LinkedBlockingQueue<ProjectLib.Message>());
    }

    /**
     * get the corresponding message queue of a specific filename
     * @param String filename
     * @return LinkedBlockingQueue<ProjectLib.Message> 
     */
    public LinkedBlockingQueue<ProjectLib.Message> getCommitQueue(String filename) {
        return commitMap.get(filename);
    }

    
    
}

