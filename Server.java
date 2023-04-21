/**
 * Server class that initiates a two-phase commit procedure to communicate with 
 * each usernode that constructs the collage. If all users agree to make the 
 * collage, the server adds the image to its directory.
 */
import java.util.*;
import java.io.*;
import java.util.concurrent.*;

public class Server implements ProjectLib.CommitServing{
	private static ProjectLib PL = null;
	private static ReceiveMessage rev;
	public static int TIMEOUT = 3000;
	private static FileReader fReader;
	private static BufferedReader reader;
	private static Map<String, String>logMap = new ConcurrentHashMap<String, String>();
	private static Map<String, String>commitMap = new ConcurrentHashMap<String, String>();
	private static File log;
	
	/**
	 * start a commit for each collage by initiating a new 2PC thread
	 * @param String filename
	 * @param byte[] img
	 * @param String[] sources
	 */
	public void startCommit( String filename, byte[] img, String[] sources ) {
		System.out.println( "Server: Got request to commit "+filename );
		CommitThread newThread = new CommitThread(filename, img, sources);
		newThread.start();
	}
	
	/**
	 * Thread class that runs the 2PC steps
	 */
	private static class CommitThread implements Runnable {
		private String filename;
		private byte[] img;
		private String[] sources;
		private String type;
		private Thread t = null;
		private Map<String, List<String>> srcMap = new ConcurrentHashMap<String, List<String>>();
		private BlockingQueue<ProjectLib.Message> queue = new LinkedBlockingQueue<ProjectLib.Message>();
		private static final Object addFile = new Object();
		private static final Object writeLog = new Object();
		private static FileWriter fWriter;
		private static BufferedWriter writer;
		
		/**
		 * Thread constructor
		 * @param String filename
		 * @param byte[] img
		 * @param String[] sources
		 */
		CommitThread(String filename, byte[] img, String[] sources) {
			this.filename = filename;
			this.img = img;
			this.sources = sources;
		}
		
		/**
		 * runs the 2PC procedure
		 */
		public void run() {
			try{
				sendVoteMessage();
				boolean result = getVoteResult();
				synchronized(writeLog) {
					fWriter = new FileWriter(log, true);
					writer = new BufferedWriter(fWriter);
					if (result) {
						synchronized(addFile) {
							FileOutputStream out = new FileOutputStream(filename);
							out.write(img);
							out.close();
							writer.write(filename + "-commit\n");
						}
					} else writer.write(filename + "-abort\n");
					writer.close();
					fWriter.close();
					PL.fsync();
				}
				sendCommitMessage(result);
				return;
			} catch (Exception e) {
				System.err.println("Exception: " + e.getMessage());
			}	
		}

		/**
		 * initialize the thread and the message receiving queues
		 */
		public void start() {
			t = new Thread(this);
			t.start();
			rev.newCommit(filename);
			this.queue = rev.getCommitQueue(filename);
		}

		/**
		 * convert an ArrayList of Strings to a single String
		 * @param List<String> sources
		 * @return concadinated string
		 */
		private String strArrayListToString(List<String> sources) {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < sources.size() - 1; i++) {
				sb.append(sources.get(i)).append(",");
			}
			sb.append(sources.get(sources.size() - 1));
			return sb.toString();
		}

		/**
		 * map each user node to its sources
		 */
		public void parseSrc() throws IOException {
			for (int i = 0; i < sources.length; i++) { // parse all sources to the map
				String[] parts = sources[i].split(":");
				String addr = parts[0];
				String srcFilename = parts[1];
				if(!srcMap.containsKey(addr)) srcMap.put(addr, new ArrayList<String>());
				srcMap.get(addr).add(srcFilename); // put the src filename into the list
			} 
		}		

		/**
		 * sends the vote starting message to each usernode for the collage.
		 * write the sources to the log and flush to disk.
		 */
		public void sendVoteMessage() throws IOException{
			StringBuffer sb = new StringBuffer();
			sb.append(filename + "-");
			for (int i = 0; i < sources.length; i++) { // parse all sources to the map
				String[] parts = sources[i].split(":");
				String addr = parts[0];
				String srcFilename = parts[1];
				if(!srcMap.containsKey(addr)) srcMap.put(addr, new ArrayList<String>());
				srcMap.get(addr).add(srcFilename); // put the src filename into the list
				sb.append(sources[i] + ";");
			} 
			synchronized(writeLog) {
				fWriter = new FileWriter(log, true);
				writer = new BufferedWriter(fWriter);
				writer.write(sb.toString() + "\n") ;
				writer.close();
				fWriter.close();
				PL.fsync();
			}
			for (Map.Entry<String, List<String>> entry : srcMap.entrySet()) {
				String id = entry.getKey();
				List<String> srcFilename = entry.getValue();
				CommitMessage msg = new CommitMessage("vote", "Server", srcFilename, img, filename);
				byte[] msgBytes = CommitMessage.makeMessage(msg);
				ProjectLib.Message msgToSend = new ProjectLib.Message(id, msgBytes);
				PL.sendMessage(msgToSend);
			}
		}

		/**
		 * Receive the vote responses fromm all usernodes. If there is a "no" or
		 * timeout, return false as the result. Otherwise, return true.
		 */
		public boolean getVoteResult() throws IOException, ClassNotFoundException{
			long start = System.currentTimeMillis();
			int voteCount = 0;
			while (System.currentTimeMillis() - start < TIMEOUT && voteCount < srcMap.size()) {
				ProjectLib.Message msg = queue.poll();
				if (msg != null) {
					CommitMessage commitMsg = CommitMessage.parseMessage(msg);
					if (commitMsg.type.equals("no")) return false;
					if (commitMsg.type.equals("yes")) voteCount += 1;
					else queue.offer(msg);
				}
			}
			if (voteCount < srcMap.size()) return false; // timeout
			return true;
		}

		/**
		 * sends the commit decision to all usernodes, then collects the response
		 * ACKs until all of them are received. Write "Finish" to the log.
		 * @param boolean result
		 */
		public void sendCommitMessage(boolean result) throws IOException, ClassNotFoundException, InterruptedException {
			while (srcMap.size() > 0) {
				for (Map.Entry<String, List<String>> entry : srcMap.entrySet()) {
					String id = entry.getKey();
					List<String> srcFilename = entry.getValue();
					String type;
					if (result) type = "commit";
					else type = "abort";
					CommitMessage msg = new CommitMessage(type, "Server", srcFilename, img, filename);
					byte[] msgBytes = CommitMessage.makeMessage(msg);
					ProjectLib.Message msgToSend = new ProjectLib.Message(id, msgBytes);
					PL.sendMessage(msgToSend);
				}
				long start = System.currentTimeMillis();
				while (System.currentTimeMillis() - start < TIMEOUT && srcMap.size() > 0) {
					ProjectLib.Message msg = queue.poll();
					if (msg != null) {
						CommitMessage commitMsg = CommitMessage.parseMessage(msg);
						if (commitMsg.type.equals("ACK")) {
							srcMap.remove(commitMsg.addr);
						}
					}
				}
			}
			synchronized(writeLog) {
				fWriter = new FileWriter(log, true);
				writer = new BufferedWriter(fWriter);
				writer.write(filename + "-Finish\n");
				writer.close();
				fWriter.close();
				PL.fsync();
			}
		}
	}

	/**
	 * read the log text and parse the collage status to log map
	 */
	public static void readLog() throws IOException {
		log = new File("LOG.txt");
		if (log.exists()) {
			fReader = new FileReader(log);
			reader = new BufferedReader(fReader);
			String line = reader.readLine();
			while (line != null) {
				String[] parts = line.split("-");
				String filename = parts[0];
				String type = parts[1];
				if (type.contains(":")) { // started vote
					commitMap.put(filename, type); // map sources with file
					logMap.put(filename, "start");
				} else if (type.equals("Finish")) { // if finished, no need to recover
					if (logMap.containsKey(filename)) logMap.remove(filename);
					if (commitMap.containsKey(filename)) commitMap.remove(filename);
				} else logMap.put(filename, type); // if commit/abort, parse result
				line = reader.readLine();
			}
		}
	}

	/**
	 * recover the previous procedures based on the log file. Send "abort" if
	 * there is no "commit" status for the corresponding collage
	 */
	public static void recover() throws IOException, ClassNotFoundException, InterruptedException {
		for (Map.Entry<String, String> entry : logMap.entrySet()) {
			String filename = entry.getKey();
			String type = entry.getValue();
			String srcs = commitMap.get(filename);
			String[] sources = srcs.split(";");
			CommitThread t = new CommitThread(filename, null, sources);
			rev.newCommit(filename);
			t.queue = rev.getCommitQueue(filename);
			t.parseSrc();
			if (type.equals("commit")) t.sendCommitMessage(true);
			else t.sendCommitMessage(false);
		}
	}

	/**
	 * main function that runs the server
	 * @param String args[]
	 */
	public static void main ( String args[] ) throws Exception {
		if (args.length != 1) throw new Exception("Need 1 arg: <port>");
		readLog();
		Server srv = new Server();
		rev = new ReceiveMessage();
		PL = new ProjectLib( Integer.parseInt(args[0]), srv,  rev);
		recover();
		log = new File("LOG.txt");
		if (!log.exists()) log.createNewFile();
		
		// main loop
		while (true) {
		}
	}
}

