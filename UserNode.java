/**
 * User node class that could receive message sent from server and make voting
 * decisions
 */
import java.util.*;
import java.io.*;
import java.nio.file.*;
import java.util.concurrent.*;


public class UserNode implements ProjectLib.MessageHandling {
	public final String myId;
	private static ProjectLib PL = null;
	private static List<String> dirty = new ArrayList<String>();
	private static final Object checkFile = new Object();
	private static final Object writeLog = new Object();
	public static File log;
	private static FileWriter fWriter;
	private static BufferedWriter writer;
	private static FileReader fReader;
	private static BufferedReader reader;
	private static Map<String, String>logMap = new ConcurrentHashMap<String, String>();
	public UserNode( String id ) {
		myId = id;
	}

	/**
	 * callback function that delivers the message from the message queue
	 * @param ProjectLib.Message msg
	 * @return boolean
	 */
	public boolean deliverMessage( ProjectLib.Message msg ) {
		try{
			CommitMessage commitMsg = CommitMessage.parseMessage(msg);
			if (commitMsg.type.equals("vote")){
				boolean result = vote(commitMsg);
				voteResponse(result, commitMsg.filename);
			} else if (commitMsg.type.equals("commit") || commitMsg.type.equals("abort")){
				commit(commitMsg);
			}
		} catch (Exception e) {
			System.err.println("Exception " + e.getMessage());
			return false;
		}
		return true;
	}

	/**
	 * make vote decisions. If a source could be used to construct the collage,
	 * it should be marked as dirty and write it to log. 
	 * @param CommitMessage msg
	 * @return boolean
	 */
	public synchronized boolean vote(CommitMessage msg) throws IOException{
		List<String> srcFilename = msg.srcFilename;
		String[] sources = srcFilename.toArray(new String[srcFilename.size()]);
		if (!PL.askUser(msg.img, sources)) return false;
		for (int i = 0; i < sources.length; i++) {
			String curFile = sources[i];
			File f = new File(curFile);
			if (dirty.contains(curFile) || !f.exists()) {
				return false;
			} 
		}
		for (int i = 0; i < sources.length; i++) {
			dirty.add(sources[i]); // add sources to dirty if vote reply is true
			fWriter = new FileWriter(log, true);
			writer = new BufferedWriter(fWriter);
			fWriter.write(sources[i] +":yes\n");
			writer.close();
			fWriter.close();
			PL.fsync();
		} 
		return true;
	}

	/**
	 * send back the vote decision to server
	 * @param boolean result
	 * @param String filename
	 */
	public void voteResponse(boolean result, String filename) throws IOException{
		String res;
		List<String> tmpList = new ArrayList<String>();
		byte[] tmpImg = new byte[0];
		if (result) res = "yes";
		else res = "no";
		CommitMessage response = new CommitMessage(res, myId, tmpList, tmpImg, filename);
		byte[] msgBytes = CommitMessage.makeMessage(response);
		ProjectLib.Message msgToSend = new ProjectLib.Message("Server", msgBytes);
		PL.sendMessage(msgToSend);
	}

	/**
	 * receive commit decision from the server. If server commits, the source
	 * files should be deleted from the directory; if server aborts, should
	 * unmark the dirty sources. Write the commit decision to log. Send an ACK
	 * message back to server.
	 * @param CommitMessage msg
	 */
	public synchronized void commit(CommitMessage msg) {
		String[] sources = msg.srcFilename.toArray(new String[msg.srcFilename.size()]);
		try {
			String type;
			for (int i = 0; i < sources.length; i++) {
				if (msg.type.equals("commit")) { // delete the dirty files if commmit
					type = "commit";
					Path source = Paths.get(sources[i]);
					Files.deleteIfExists(source);
				} else type = "abort";
				if (dirty.contains(sources[i])) {
					dirty.remove(sources[i]);
				}
				fWriter = new FileWriter(log, true);
				writer = new BufferedWriter(fWriter);
				fWriter.write(sources[i] + ":" + type + "\n");
				writer.close();
				fWriter.close();
				PL.fsync();
			}
			List<String> tmpList = new ArrayList<String>();
			byte[] tmpImg = new byte[0];
			CommitMessage response = new CommitMessage("ACK", myId, tmpList, tmpImg, msg.filename);
			byte[] msgBytes = CommitMessage.makeMessage(response);
			ProjectLib.Message msgToSend = new ProjectLib.Message("Server", msgBytes);
			PL.sendMessage(msgToSend);
		} catch (IOException e) {
			System.err.println(e.getMessage());
		}
	}

	/**
	 * read the log text and recover the previous procedures based on the log 
	 * file.
	 * @param String id
	 */
	public static void recover(String id) throws IOException{
		log = new File("LOG.txt");
		if (log.exists()) {
			fReader = new FileReader(log);
			reader = new BufferedReader(fReader);
			String line = reader.readLine();
			while (line != null) {
				String[] parts = line.split(":");
				if (parts[1].equals("yes")) logMap.put(parts[0], "voted");
				else logMap.put(parts[0], parts[-1]);
				line = reader.readLine();
			}
			for (Map.Entry<String, String> entry : logMap.entrySet()) {
			 	String srcFilename = entry.getKey();
			 	String type = entry.getValue();
			 	if (type.equals("voted")) dirty.add(srcFilename);
			}
		}
	}

	/**
	 * main function that runs each usernode
	 * @param String args[]
	 */
	public static void main ( String args[] ) throws Exception {
		if (args.length != 2) throw new Exception("Need 2 args: <port> <id>");
		recover(args[1]);
		UserNode UN = new UserNode(args[1]);
		PL = new ProjectLib( Integer.parseInt(args[0]), args[1], UN );
		log = new File("LOG.txt");
		if (!log.exists()) log.createNewFile();
		
	}
}

