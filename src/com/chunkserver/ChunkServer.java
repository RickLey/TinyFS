package com.chunkserver;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
//import java.util.Arrays;

import com.client.Client;
import com.interfaces.ChunkServerInterface;
import com.master.Master;

/**
 * implementation of interfaces at the chunkserver side
 * @author Shahram Ghandeharizadeh
 *
 */

public class ChunkServer implements ChunkServerInterface {
	final static String filePath = "csci485/";	//or C:\\newfile.txt
	public final static String ClientConfigFile = "ClientConfig.txt";
	
	//Used for the file system
	public static long counter;
	
	public static int PayloadSZ = Integer.SIZE/Byte.SIZE;  //Number of bytes in an integer
	public static int CMDlength = Integer.SIZE/Byte.SIZE;  //Number of bytes in an integer  
	
	//Commands recognized by the Server
	public static final int CreateChunkCMD = 101;
	public static final int ReadChunkCMD = 102;
	public static final int WriteChunkCMD = 103;
	
	//Replies provided by the server
	public static final int TRUE = 1;
	public static final int FALSE = 0;
	
	public static int SERVER_PORT = 8081;
	
	/*
	 * Available size for records is MAX_CHUNKSIZE - 3 because of metadata (2 bytes for length of record and 1 byte for (in)valid)
	 */
	
	
	/**
	 * Initialize the chunk server
	 */
	public ChunkServer(){
		File dir = new File(filePath);
		if (!dir.exists()) {
			dir.mkdir();
		}
		File[] fs = dir.listFiles();

		if(fs.length == 0){
			counter = 0;
		}else{
			long[] cntrs = new long[fs.length];
			for (int j=0; j < cntrs.length; j++){
				if(fs[j].getName().equals(".DS_Store")){
					continue;
				}
				cntrs[j] = Long.valueOf( fs[j].getName() ); 
			}
			Arrays.sort(cntrs);
			counter = cntrs[cntrs.length - 1];
		}

		// register with master
		Socket masterSocket = null;
		try {
			masterSocket = new Socket(Master.HOST, Master.PORT);
			DataOutputStream out = new DataOutputStream(new BufferedOutputStream(masterSocket.getOutputStream()));
			byte[] host = InetAddress.getLocalHost().getHostAddress().getBytes();
			//String host = "localhost";
			out.writeInt(12 + host.length);
			out.writeInt(Master.REGISTER_CHUNKSERVER_CMD);
			out.writeInt(host.length);
			out.write(host);
			out.writeInt(SERVER_PORT);
			out.flush();
			System.out.println("Connected to master");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				masterSocket.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * Each chunk is corresponding to a file.
	 * Return the chunk handle of the last chunk in the file.
	 */
	public String createChunk() {
		counter++;
		return String.valueOf(counter);
	}
	
	/**
	 * Write the byte array to the chunk at the offset
	 * The byte array size should be no greater than 4KB
	 */
	public boolean writeChunk(String ChunkHandle, byte[] payload, int offset) {
		try {
			//If the file corresponding to ChunkHandle does not exist then create it before writing into it
			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.write(payload, 0, payload.length);
			raf.close();
			return true;
		} catch (IOException ex) {
			ex.printStackTrace();
			return false;
		}
	}
	
	/**
	 * read the chunk at the specific offset
	 */
	public byte[] readChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		try {
			//If the file for the chunk does not exist the return null
			boolean exists = (new File(filePath + ChunkHandle)).exists();
			if (exists == false) return null;
			
			//File for the chunk exists then go ahead and read it
			byte[] data = new byte[NumberOfBytes];
			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.read(data, 0, NumberOfBytes);
			raf.close();
			return data;
		} catch (IOException ex){
			ex.printStackTrace();
			return null;
		}
	}

	private static class RequestProcessor implements Runnable {
		private ChunkServer cs = null;
		private Socket socket = null;
		private ObjectOutputStream WriteOutput = null;
		private ObjectInputStream ReadInput = null;

		public RequestProcessor(ChunkServer cs, Socket socket) {
			this.cs = cs;
			this.socket = socket;
			try {
				this.ReadInput = new ObjectInputStream(socket.getInputStream());
				this.WriteOutput = new ObjectOutputStream(socket.getOutputStream());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		public void run() {
			Socket ClientConnection = null;  //A client's connection to the server
			try {
				//Use the existing input and output stream as long as the client is connected
				while (!socket.isClosed()) {
					int payloadsize =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
					if (payloadsize == -1)
						break;
					int CMD = Client.ReadIntFromInputStream("ChunkServer", ReadInput);
					switch (CMD){
						case CreateChunkCMD:
							String chunkhandle = cs.createChunk();
							//byte[] CHinbytes = chunkhandle.getBytes();
							//WriteOutput.writeInt(ChunkServer.PayloadSZ + CHinbytes.length);
							WriteOutput.writeObject(chunkhandle);
							WriteOutput.flush();
							break;

						case ReadChunkCMD:
							int offset =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
							int payloadlength =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
							int chunkhandlesize = payloadsize - ChunkServer.PayloadSZ - ChunkServer.CMDlength - (2 * 4);
							if (chunkhandlesize < 0)
								System.out.println("Error in ChunkServer.java, ReadChunkCMD has wrong size.");
							byte[] CHinBytes = Client.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
							String ChunkHandle = (new String(CHinBytes)).toString();

							byte[] res = cs.readChunk(ChunkHandle, offset, payloadlength);

							if (res == null)
								WriteOutput.writeInt(ChunkServer.PayloadSZ);
							else {
								WriteOutput.writeInt(ChunkServer.PayloadSZ + res.length);
								WriteOutput.write(res);
							}
							WriteOutput.flush();
							break;

						case WriteChunkCMD:
							offset =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
							payloadlength =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
							byte[] payload = Client.RecvPayload("ChunkServer", ReadInput, payloadlength);
							chunkhandlesize = payloadsize - ChunkServer.PayloadSZ - ChunkServer.CMDlength - (2 * 4) - payloadlength;
							if (chunkhandlesize < 0)
								System.out.println("Error in ChunkServer.java, WritehChunkCMD has wrong size.");
							CHinBytes = Client.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
							ChunkHandle = (new String(CHinBytes)).toString();

							//Call the writeChunk command
							if (cs.writeChunk(ChunkHandle, payload, offset))
								WriteOutput.writeInt(ChunkServer.TRUE);
							else WriteOutput.writeInt(ChunkServer.FALSE);

							WriteOutput.flush();
							break;

						default:
							System.out.println("Error in ChunkServer, specified CMD "+CMD+" is not recognized.");
							break;
					}
				}
			} catch (IOException ex){
				System.out.println("Client Disconnected");
			} finally {
				try {
					if (socket != null)
						socket.close();
					if (ReadInput != null)
						ReadInput.close();
					if (WriteOutput != null) WriteOutput.close();
				} catch (IOException fex){
					System.out.println("Error (ChunkServer):  Failed to close either a valid connection or its input/output stream.");
					fex.printStackTrace();
				}
			}
		}
	}
	


	public static void main(String args[]) {
		ChunkServer cs = new ChunkServer();

		int ServerPort = 0; //Set to 0 to cause ServerSocket to allocate the port
		ServerSocket commChanel = null;

		try {
			//Allocate a port and write it to the config file for the Client to consume
			commChanel = new ServerSocket(SERVER_PORT); //TODO:hardcoded so far!!!! must change
			//ServerPort=commChanel.getLocalPort();
			PrintWriter outWrite=new PrintWriter(new FileOutputStream(ClientConfigFile));
			outWrite.println("localhost:"+ServerPort);
			outWrite.close();
		} catch (IOException ex) {
			System.out.println("Error, failed to open a new socket to listen on.");
			ex.printStackTrace();
		}

		Socket socket = null;
		ArrayList<Thread> threads = new ArrayList<Thread>();
		while (true) {
			try {
				socket = commChanel.accept();
				Thread thread = new Thread(new ChunkServer.RequestProcessor(cs, socket));
				thread.start();
				threads.add(thread);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
