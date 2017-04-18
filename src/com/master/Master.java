package com.master;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

import com.chunkserver.ChunkServer;
import com.client.FileHandle;
import com.client.ClientFS;
import com.client.ClientFS.FSReturnVals;
public class Master implements Serializable, Runnable{

	public static int PORT = 1234;
	public static String HOST = "localhost";

	public static final int CREATE_DIR_CMD = 101;
	public static final int DELETE_DIR_CMD = 102;
	public static final int RENAME_DIR_CMD = 103;
	public static final int LIST_DIR_CMD = 104;
	public static final int CREATE_FILE_CMD = 105;
	public static final int DELETE_FILE_CMD = 106;
	public static final int OPEN_FILE_CMD = 107;
	public static final int CLOSE_FILE_CMD = 108;
	public static final int REGISTER_CHUNKSERVER_CMD = 109;
	public static final int VERIFY_FILE_HANDLE_CMD = 110;
	public static final int GET_HANDLE_FOR_APPEND_CMD = 111;
	public static final int GET_FIRST_CHUNK_HANDLE_FOR_FILE_CMD = 112;
	public static final int GET_LAST_CHUNK_HANDLE_FOR_FILE_CMD = 113;
	public static final int GET_NEXT_CHUNK_HANDLE_CMD = 114;

	public static final String stateFile = "state";
	
	private static ArrayList<String> namespace; //Ordered list of FileHandles(String)
	private static HashMap<String, ArrayList<String>> chunkLists;	//Map from FileHandle to list of chunkHandle's
	private static HashMap<String, String> chunkLocations; //Map from chunkHandle to chunkServer IPs
	private static HashMap<String, Integer> remainingChunkSpace; // How much space is left in last chunk of a file

	private static ReentrantLock namespaceLock;

	private static HashMap<String, Socket> chunkservers;
	private static ArrayDeque<String> chunkserverQueue;

	private Socket connection;
	
	private void initializeDataStructures()
	{
		namespace = new ArrayList<String>();
		chunkLists = new HashMap<String, ArrayList<String>>();
		chunkLocations = new HashMap<String, String>();
		remainingChunkSpace = new HashMap<String, Integer>();
		namespace.add("/");
		chunkservers = new HashMap<String, Socket>();
		chunkserverQueue = new ArrayDeque<String>();
	}
	public Master(Socket socket)
	{
		if(namespace == null)
		{
			namespaceLock = new ReentrantLock();
			try {
				loadState();
			} catch (ClassNotFoundException | IOException e) {
				initializeDataStructures();
			}
		}
		
		connection = socket;
	}
	
	/**
	 * Creates the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: CreateDir("/", "Shahram"), CreateDir("/Shahram/",
	 * "CSCI485"), CreateDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals CreateDir(String src, String dirname) {
		namespaceLock.lock();

		int parentIndex = namespace.indexOf(src);
		if(parentIndex < 0)
		{
			return ClientFS.FSReturnVals.SrcDirNotExistent;
		}
		
		String fullPath = src + dirname + "/";
		if(namespace.indexOf(fullPath) >= 0)
		{
			return ClientFS.FSReturnVals.DestDirExists;
		}
		
		int index = parentIndex;
		while(index < namespace.size() && fullPath.compareTo(namespace.get(index)) > 0)
		{
			index++; // Find the index at which this subdir should be inserted
		}
		namespace.add(index, fullPath);
		// No chunkLists entry because directories do not consist of chunks
		
		saveState();

		namespaceLock.unlock();

		return ClientFS.FSReturnVals.Success;
	}

	/**
	 * Deletes the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: DeleteDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals DeleteDir(String src, String dirname) {
		namespaceLock.lock();

		int parentIndex = namespace.indexOf(src);
		if(parentIndex < 0)
		{
			return ClientFS.FSReturnVals.SrcDirNotExistent;
		}
		
		String fullPath = src + dirname + "/";
		int dirIndex = namespace.indexOf(fullPath);
		if(dirIndex < 0)
		{
			return ClientFS.FSReturnVals.Fail; // Dir doesn't exist
		}
		
		if(namespace.get(dirIndex + 1).startsWith(fullPath))
		{
			return ClientFS.FSReturnVals.DirNotEmpty;
		}
		
		namespace.remove(dirIndex);
		saveState();
		namespaceLock.unlock();
		

		return ClientFS.FSReturnVals.Success;
	}

	/**
	 * Renames the specified src directory in the specified path to NewName
	 * Returns SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if a directory with NewName exists in the specified path
	 *
	 * Example usage: RenameDir("/Shahram/CSCI485", "/Shahram/CSCI550") changes
	 * "/Shahram/CSCI485" to "/Shahram/CSCI550"
	 */
	public FSReturnVals RenameDir(String src, String NewName) {
		namespaceLock.lock();

		int index = namespace.indexOf(src + "/");
		if(index < 0)
		{
			namespaceLock.unlock();
			return ClientFS.FSReturnVals.SrcDirNotExistent;
		}
		
		if(namespace.indexOf(NewName + "/") >= 0)
		{
			namespaceLock.unlock();
			return ClientFS.FSReturnVals.DestDirExists;
		}
		
		String temp = namespace.get(index);
		while(temp.startsWith(src + "/"))
		{
			String renamed = temp.replaceFirst(src, NewName);
			namespace.remove(index);
			namespace.add(index, renamed);
			ArrayList<String> list = chunkLists.get(temp);
			if(list != null) // Only for files, not directories
			{
				chunkLists.remove(temp);
				chunkLists.put(renamed, list);
				
				int space = remainingChunkSpace.get(temp); // Because the entry exists in ChunkLists, it will exist here
				remainingChunkSpace.remove(temp);
				remainingChunkSpace.put(renamed, space);
			}
			
			index++;
			if(index == namespace.size())
			{
				break;
			}
			temp = namespace.get(index);
		}

		namespaceLock.unlock();

		// Must sort at the end to maintain canonical order? -- shouldn't be necessary
		// as they will all still be clustered
		
		saveState();
		return ClientFS.FSReturnVals.Success;
	}

	/**
	 * Lists the content of the target directory Returns SrcDirNotExistent if
	 * the target directory does not exist Returns null if the target directory
	 * is empty
	 *
	 * Example usage: ListDir("/Shahram/CSCI485")
	 */
	public String[] ListDir(String tgt) {
		namespaceLock.lock();

		// Iterate through ordered list until there's a prefix that is not the target dir
		int index = namespace.indexOf(tgt + "/");
		if(index < 0)
		{
			namespaceLock.unlock();
			return null;
		}
		
		ArrayList<String> result = new ArrayList<String>();
		index += 1;
		if(index < namespace.size())
		{
			String temp = namespace.get(index);
			while(temp.startsWith(tgt))
			{
				if(temp.endsWith("/"))
				{
					/* We add a '/' to the end of directory names to differentiate them from files
					 * when checking if a directory exists. The unit tests do not expect this,
					 * so the directory names must be trimmed
					*/ 
					temp = temp.substring(0, temp.length() - 1);
				}
				result.add(temp);
				index++;
				if(index == namespace.size())
				{
					break;
				}
				temp = namespace.get(index);
			}

		}

		namespaceLock.unlock();
				
		if(result.size() == 0)
		{
			return null;
		}
		return result.toArray(new String[1]); // force the function to just create a new array
	}

	/**
	 * Creates the specified filename in the target directory Returns
	 * SrcDirNotExistent if the target directory does not exist Returns
	 * FileExists if the specified filename exists in the specified directory
	 *
	 * Example usage: Createfile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals CreateFile(String tgtdir, String filename) {
		// Send message to master
		// Master will insert filename into ordered list
		int parentIndex = namespace.indexOf(tgtdir);
		if(parentIndex < 0)
		{
			return ClientFS.FSReturnVals.SrcDirNotExistent;
		}
		String fullPath = tgtdir + filename;
		if(namespace.indexOf(fullPath) >= 0)
		{
			return ClientFS.FSReturnVals.FileExists;
		}
		
		int index = parentIndex;
		while(index < namespace.size() && fullPath.compareTo(namespace.get(index)) > 0)
		{
			index++; // Find the index at which this file should be inserted
		}
		namespace.add(index, fullPath);
		chunkLists.put(fullPath, new ArrayList<String>());
		remainingChunkSpace.put(fullPath, 0); // Because no chunk allocated yet.
		// Files are handled the same as directories. All directories end with a /
		saveState();
		return ClientFS.FSReturnVals.Success;
	}

	/**
	 * Deletes the specified filename from the tgtdir Returns SrcDirNotExistent
	 * if the target directory does not exist Returns FileDoesNotExist if the
	 * specified filename is not-existent
	 *
	 * Example usage: DeleteFile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals DeleteFile(String tgtdir, String filename) {
		int parentIndex = namespace.indexOf(tgtdir);
		if(parentIndex < 0)
		{
			return ClientFS.FSReturnVals.SrcDirNotExistent;
		}
		
		String fullPath = tgtdir + filename;
		int index = namespace.indexOf(fullPath);
		if(index < 0)
		{
			return ClientFS.FSReturnVals.FileDoesNotExist;
		}
		
		namespace.remove(index);
		ArrayList<String> chunkList = chunkLists.get(fullPath);
		for(String chunk : chunkList)
		{
			chunkLocations.remove(chunk);
		}
		chunkLists.remove(fullPath);
		remainingChunkSpace.remove(fullPath);
		
		saveState();
		
		// Tell chunkservers to delete those files?
		
		return ClientFS.FSReturnVals.Success;
	}

	/**
	 * Opens the file specified by the FilePath and populates the FileHandle
	 * Returns FileDoesNotExist if the specified filename by FilePath is
	 * not-existent
	 *
	 * Example usage: OpenFile("/Shahram/CSCI485/Lecture1/Intro.pptx", FH1)
	 */
	public FSReturnVals OpenFile(String FilePath, FileHandle ofh) {
		if(namespace.indexOf(FilePath) < 0)
		{
			return ClientFS.FSReturnVals.FileDoesNotExist;
		}
		ofh.filename = FilePath;
		return ClientFS.FSReturnVals.Success;
	}

	/**
	 * Closes the specified file handle Returns BadHandle if ofh is invalid
	 *
	 * Example usage: CloseFile(FH1)
	 */
	public FSReturnVals CloseFile(FileHandle ofh) {
		if(namespace.indexOf(ofh.filename) < 0)
		{
			return ClientFS.FSReturnVals.BadHandle;
		}
		return ClientFS.FSReturnVals.Success;
	}

	

	
	public boolean VerifyFileHandle(String fileHandle)
	{
		return !chunkLists.containsKey(fileHandle);
	}
	
	boolean VerifyFileHandleAndChunkHandle(String fileHandle, String chunkHandle){
		return VerifyFileHandle(fileHandle) ||
				chunkLists.get(fileHandle).indexOf(chunkHandle) >= 0;
	}
	
	/*
	 * Assumes file and chunk handle have already been validated
	 */
	String GetNextChunkHandle(String fileHandle, String ChunkHandle){
		ArrayList<String> list = chunkLists.get(fileHandle);
		int index = list.indexOf(ChunkHandle) + 1;
		return index < list.size() ? list.get(index) : null;
	}
	
	String GetPreviousChunkHandle(String fileHandle, String ChunkHandle){
		ArrayList<String> list = chunkLists.get(fileHandle);
		int index = list.indexOf(ChunkHandle) - 1;
		return index > 0 ? list.get(index) : null;
	}
	
	String GetLastChunkHandleOfAFile(String FileHandle){
		ArrayList<String> list = chunkLists.get(FileHandle);
		return list.size() > 0 ? list.get(list.size() - 1) : null;
	}
	
	String GetFirstChunkHandleForFile(String FileHandle){
		ArrayList<String> list = chunkLists.get(FileHandle);
		return list.size() > 0 ? list.get(0) : null;
	}
	
	String GetHandleForAppend(String FileHandle, Integer payloadSize){
		int remainingSpace = remainingChunkSpace.get(FileHandle);
		if(payloadSize < remainingSpace)
		{
			ArrayList<String> list = chunkLists.get(FileHandle);
			remainingChunkSpace.put(FileHandle, remainingSpace - payloadSize);
			saveState();
			return list.get(list.size() - 1);
		}
		else
		{
			try {
				String host = nextChunkserver();
				Socket socket = chunkservers.get(host);
				DataOutputStream out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
				DataInputStream in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));

				out.writeInt(8);
				out.writeInt(ChunkServer.CreateChunkCMD);
				out.flush();

				int handleSize = in.readInt() - ChunkServer.PayloadSZ;
				String chunkHandle = readString(in, handleSize);
				chunkLocations.put(chunkHandle, host);
				chunkLists.get(FileHandle).add(chunkHandle); // add the handle to the list for this file
				remainingChunkSpace.put(FileHandle, ChunkServer.ChunkSize - payloadSize); // reset the remaining space to be chunksize - payloadsize
				return chunkHandle;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}
	}

	private String nextChunkserver() {
		if (chunkserverQueue.isEmpty()) {
			return null;
		}

		String next = chunkserverQueue.removeFirst();
		chunkserverQueue.addLast(next);
		return next;
	}

	private String readString(DataInputStream in, int size) throws IOException {
		byte[] bytes = new byte[size];
		int bytesRead = 0;
		while (bytesRead < size) {
			bytesRead += in.read(bytes, bytesRead, size - bytesRead);
		}
		return new String(bytes);
	}

	/**
	 *  CREATE_DIR_CMD Packet Layout
	 *
	 * 	0-3		packet size
	 * 	4-7		command
	 * 	8-11	src size
	 * 	12-15	dirname size
	 * 	...		src
	 * 	...		dirname
	 *
	 * 	0-3		packet size
	 * 	4-7		return value
	 *
	 */
	public void handleCreateDirCmd(DataInputStream in, DataOutputStream out) throws IOException {
		int srcSize = in.readInt();
		int dirnameSize = in.readInt();

		String src = readString(in, srcSize);
		String dirname = readString(in, dirnameSize);

		FSReturnVals returnVal = CreateDir(src, dirname);

		out.writeInt(8);
		out.writeInt(returnVal.getValue());
	}

	/**
	 *  DELETE_DIR_CMD Packet Layout
	 *
	 * 	0-3		packet size
	 * 	4-7		command
	 * 	8-11	src size
	 * 	12-15	dirname size
	 * 	...		src
	 * 	...		dirname
	 *
	 * 	0-3		packet size
	 * 	4-7		return value
	 *
	 */
	public void handleDeleteDirCmd(DataInputStream in, DataOutputStream out) throws IOException {
		int srcSize = in.readInt();
		int dirnameSize = in.readInt();

		String src = readString(in, srcSize);
		String dirname = readString(in, dirnameSize);

		FSReturnVals returnVal = DeleteDir(src, dirname);

		out.writeInt(8);
		out.writeInt(returnVal.getValue());
	}

	/**
	 *  RENAME_DIR_CMD Packet Layout
	 *
	 * 	0-3		packet size
	 * 	4-7		command
	 * 	8-11	src size
	 * 	12-15	newname size
	 * 	...		src
	 * 	...		newname
	 *
	 * 	0-3		packet size
	 * 	4-7		return value
	 *
	 */
	public void handleRenameDirCmd(DataInputStream in, DataOutputStream out) throws IOException {
		int srcSize = in.readInt();
		int newnameSize = in.readInt();

		String src = readString(in, srcSize);
		String newname = readString(in, newnameSize);

		FSReturnVals returnVal = RenameDir(src, newname);

		out.writeInt(8);
		out.writeInt(returnVal.getValue());
	}

	/**
	 *  LIST_DIR_CMD Packet Layout
	 *
	 * 	0-3		packet size
	 * 	4-7		command
	 * 	8-11	target size
	 * 	...		target
	 *
	 * 	0-3		packet size
	 * 	4-7		number of listings (n)
	 * 	8-11	length of string F1
	 * 	...		string F1
	 * 			...
	 * 	...		length of string Fn
	 * 	...		string Fn
	 *
	 */
	public void handleListDirCmd(DataInputStream in, DataOutputStream out) throws IOException {
		int targetSize = in.readInt();

		String target = readString(in, targetSize);

		String[] listings = ListDir(target);

		if (listings == null) {
			out.writeInt(4);
		} else {
			int returnPacketSize = 8;
			for (int i = 0 ; i < listings.length ; i++) {
				returnPacketSize += 4 + listings[i].getBytes().length;
			}
			out.writeInt(returnPacketSize);

			out.writeInt(listings.length);
			for (int i = 0 ; i < listings.length ; i++) {
				byte[] listingBytes = listings[i].getBytes();
				out.writeInt(listingBytes.length);
				out.write(listingBytes);
			}
		}
	}

	/**
	 *  CREATE_FILE_CMD Packet Layout
	 *
	 * 	0-3		packet size
	 * 	4-7		command
	 * 	8-11	tgtdir size
	 * 	12-15	filename size
	 * 	...		tgtdir
	 * 	...		filename
	 *
	 * 	0-3		packet size
	 * 	4-7		return value
	 *
	 */
	public void handleCreateFileCmd(DataInputStream in, DataOutputStream out) throws IOException {
		int tgtdirSize = in.readInt();
		int filenameSize = in.readInt();

		String tgtdir = readString(in, tgtdirSize);
		String filename = readString(in, filenameSize);

		FSReturnVals returnVal = CreateFile(tgtdir, filename);

		out.writeInt(8);
		out.writeInt(returnVal.getValue());
	}

	/**
	 *  DELETE_FILE_CMD Packet Layout
	 *
	 * 	0-3		packet size
	 * 	4-7		command
	 * 	8-11	tgtdir size
	 * 	12-15	filename size
	 * 	...		tgtdir
	 * 	...		filename
	 *
	 * 	0-3		packet size
	 * 	4-7		return value
	 *
	 */
	public void handleDeleteFileCmd(DataInputStream in, DataOutputStream out) throws IOException {
		int tgtdirSize = in.readInt();
		int filenameSize = in.readInt();

		String tgtdir = readString(in, tgtdirSize);
		String filename = readString(in, filenameSize);

		FSReturnVals returnVal = DeleteFile(tgtdir, filename);

		out.writeInt(8);
		out.writeInt(returnVal.getValue());
	}

	/**
	 *  OPEN_FILE_CMD Packet Layout
	 *
	 * 	0-3		packet size
	 * 	4-7		command
	 * 	8-11	filepath size
	 * 	...		filepath
	 *
	 * 	0-3		packet size
	 * 	4-7		return value
	 * 	8-11	filename length
	 * 	...		filename
	 *
	 */
	public void handleOpenFileCmd(DataInputStream in, DataOutputStream out) throws IOException {
		int filepathSize = in.readInt();

		String filepath = readString(in, filepathSize);

		FileHandle handle = new FileHandle();
		FSReturnVals returnVal = OpenFile(filepath, handle);

		if (returnVal == FSReturnVals.Success) {
			byte[] filenameBytes = handle.filename.getBytes();
			out.writeInt(12 + filenameBytes.length);
			out.writeInt(returnVal.getValue());
			out.writeInt(filenameBytes.length);
			out.write(filenameBytes);
		} else {
			out.writeInt(8);
			out.writeInt(returnVal.getValue());
		}
	}

	/**
	 *  CLOSE_FILE_CMD Packet Layout
	 *
	 * 	0-3		packet size
	 * 	4-7		command
	 * 	8-11	filepath size
	 * 	12-15	filename size
	 * 	...		filepath
	 * 	...		filename
	 *
	 * 	0-3		packet size
	 * 	4-7		return value
	 *
	 */
	public void handleCloseFileCmd(DataInputStream in, DataOutputStream out) throws IOException {
		int filenameSize = in.readInt();

		String filename = readString(in, filenameSize);

		FSReturnVals returnVal = CloseFile(new FileHandle(filename));

		out.writeInt(8);
		out.writeInt(returnVal.getValue());
	}

	/**
	 *  REGISTER_CHUNKSERVER_CMD Packet Layout
	 *
	 * 	0-3		packet size
	 * 	4-7		command
	 * 	8-11	host size
	 * 	...		host
	 *
	 * 	(no return packet)
	 *
	 */
	public void handleRegisterChunkserverCmd(DataInputStream in, Socket socket) throws IOException {
		int hostSize = in.readInt();
		String host = readString(in, hostSize);
		chunkservers.put(host, socket);
		chunkserverQueue.addLast(host);
	}

	/**
	 *  VERIFY_FILE_HANDLE_CMD Packet Layout
	 *
	 * 	0-3		packet size
	 * 	4-7		command
	 * 	8-11	filehandle size
	 * 	...		filehandle
	 *
	 * 	0-3		packet size
	 * 	4		boolean
	 *
	 */
	public void handleVerifyFileHandleCmd(DataInputStream in, DataOutputStream out) throws IOException {
		int filehandleSize = in.readInt();
		String filehandle = readString(in, filehandleSize);

		boolean success = VerifyFileHandle(filehandle);

		out.writeInt(5);
		out.writeBoolean(success);
	}

	/**
	 *  GET_HANDLE_FOR_APPEND_CMD Packet Layout
	 *
	 * 	0-3		packet size
	 * 	4-7		command
	 * 	8-11	payload size
	 * 	12-15	filehandle size
	 * 	...		filehandle
	 *
	 * 	0-3		packet size
	 * 	4-7		chunkhandle size
	 * 	...		chunkhandle
	 *
	 */
	public void handleGetHandleForAppendCmd(DataInputStream in, DataOutputStream out) throws IOException {
		int payloadSize = in.readInt();

		int filehandleSize = in.readInt();
		String filehandle = readString(in, filehandleSize);

		String chunkHandle = GetHandleForAppend(filehandle, payloadSize);
		if (chunkHandle == null) {
			out.writeInt(4);
			return;
		}

		byte[] chunkHandleBytes = chunkHandle.getBytes();
		out.writeInt(8 + chunkHandleBytes.length);
		out.writeInt(chunkHandleBytes.length);
		out.write(chunkHandleBytes);
	}

	/**
	 *  GET_FIRST_CHUNK_HANDLE_FOR_FILE_CMD Packet Layout
	 *
	 * 	0-3		packet size
	 * 	4-7		command
	 * 	8-11	filehandle size
	 * 	...		filehandle
	 *
	 * 	0-3		packet size
	 * 	4-7		chunkhandle size
	 * 	...		chunkhandle
	 *
	 */
	public void handleGetFirstChunkHandleForFileCmd(DataInputStream in, DataOutputStream out) throws IOException {
		int filehandleSize = in.readInt();
		String filehandle = readString(in, filehandleSize);

		String chunkHandle = GetFirstChunkHandleForFile(filehandle);
		if (chunkHandle == null) {
			out.writeInt(4);
			return;
		}

		byte[] chunkHandleBytes = chunkHandle.getBytes();
		out.writeInt(8 + chunkHandleBytes.length);
		out.writeInt(chunkHandleBytes.length);
		out.write(chunkHandleBytes);
	}

	/**
	 *  GET_LAST_CHUNK_HANDLE_FOR_FILE_CMD Packet Layout
	 *
	 * 	0-3		packet size
	 * 	4-7		command
	 * 	8-11	filehandle size
	 * 	...		filehandle
	 *
	 * 	0-3		packet size
	 * 	4-7		chunkhandle size
	 * 	...		chunkhandle
	 *
	 */
	public void handleGetLastChunkHandleForFileCmd(DataInputStream in, DataOutputStream out) throws IOException {
		int filehandleSize = in.readInt();
		String filehandle = readString(in, filehandleSize);

		String chunkHandle = GetLastChunkHandleOfAFile(filehandle);
		if (chunkHandle == null) {
			out.writeInt(4);
			return;
		}

		byte[] chunkHandleBytes = chunkHandle.getBytes();
		out.writeInt(8 + chunkHandleBytes.length);
		out.writeInt(chunkHandleBytes.length);
		out.write(chunkHandleBytes);
	}

	/**
	 *  GET_NEXT_CHUNK_HANDLE_CMD Packet Layout
	 *
	 * 	0-3		packet size
	 * 	4-7		command
	 * 	8-11	filehandle size
	 * 	12-15	chunkhandle size
	 * 	...		filehandle
	 * 	...		chunkhandle size
	 *
	 * 	0-3		packet size
	 * 	4-7		chunkhandle size
	 * 	...		chunkhandle
	 *
	 */
	public void handleGetNextChunkHandleCmd(DataInputStream in, DataOutputStream out) throws IOException {
		int filehandleSize = in.readInt();
		int chunkhandleSize = in.readInt();

		String filehandle = readString(in, filehandleSize);
		String chunkhandle = readString(in, chunkhandleSize);

		String nextChunkHandle = GetNextChunkHandle(filehandle, chunkhandle);
		if (nextChunkHandle == null) {
			out.writeInt(4);
			return;
		}

		byte[] chunkHandleBytes = nextChunkHandle.getBytes();
		out.writeInt(8 + chunkHandleBytes.length);
		out.writeInt(chunkHandleBytes.length);
		out.write(chunkHandleBytes);
	}

	@Override
	public void run() {
		try {
			DataOutputStream out = new DataOutputStream(new BufferedOutputStream(connection.getOutputStream()));
			DataInputStream in = new DataInputStream(new BufferedInputStream(connection.getInputStream()));

			while (!connection.isClosed()) {
				while (in.available() <= 0);
				int packetSize = in.readInt();
				if (packetSize == -1) {
					break;
				}

				int command = in.readInt();
				switch (command) {
					case CREATE_DIR_CMD:
						handleCreateDirCmd(in, out);
						break;

					case DELETE_DIR_CMD:
						handleDeleteDirCmd(in, out);
						break;

					case RENAME_DIR_CMD:
						handleRenameDirCmd(in, out);
						break;

					case LIST_DIR_CMD:
						handleListDirCmd(in, out);
						break;

					case CREATE_FILE_CMD:
						handleCreateFileCmd(in, out);
						break;

					case DELETE_FILE_CMD:
						handleDeleteFileCmd(in, out);
						break;

					case OPEN_FILE_CMD:
						handleOpenFileCmd(in, out);
						break;

					case CLOSE_FILE_CMD:
						handleCloseFileCmd(in, out);
						break;

					case REGISTER_CHUNKSERVER_CMD:
						handleRegisterChunkserverCmd(in, connection);
						break;

					case VERIFY_FILE_HANDLE_CMD:
						handleVerifyFileHandleCmd(in, out);
						break;

					case GET_HANDLE_FOR_APPEND_CMD:
						handleGetHandleForAppendCmd(in, out);
						break;

					case GET_FIRST_CHUNK_HANDLE_FOR_FILE_CMD:
						handleGetFirstChunkHandleForFileCmd(in, out);
						break;

					case GET_LAST_CHUNK_HANDLE_FOR_FILE_CMD:
						handleGetLastChunkHandleForFileCmd(in, out);
						break;

					case GET_NEXT_CHUNK_HANDLE_CMD:
						handleGetNextChunkHandleCmd(in, out);
						break;
				}

				out.flush();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void saveState()
	{
		FileOutputStream fos;
		ObjectOutputStream oos;
		try {
			fos = new FileOutputStream(stateFile);
			oos = new ObjectOutputStream(fos);
			oos.writeObject(namespace);
			oos.writeObject(chunkLists);
			oos.writeObject(chunkLocations);
			oos.writeObject(remainingChunkSpace);
			oos.writeObject(chunkservers);
			oos.writeObject(chunkserverQueue);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void loadState() throws IOException, ClassNotFoundException
	{
		FileInputStream fis;
		ObjectInputStream ois;
		fis = new FileInputStream(stateFile);
		ois = new ObjectInputStream(fis);
		namespace = (ArrayList<String>) ois.readObject();
		chunkLists = (HashMap<String, ArrayList<String>>) ois.readObject();
		chunkLocations = (HashMap<String, String>) ois.readObject();
		remainingChunkSpace = (HashMap<String, Integer>) ois.readObject();
		chunkservers = (HashMap<String, Socket>) ois.readObject();
		chunkserverQueue = (ArrayDeque<String>) ois.readObject();
	}

	public static void main(String[] args) {
		Master intialMaster = new Master(null);

		try {
			ServerSocket server = new ServerSocket(Master.PORT);

			Socket connection = null;

			ArrayList<Thread> threads = new ArrayList<Thread>();

			while (true) {
				connection = server.accept();
				Master master = new Master(connection);
				Thread thread = new Thread(master);
				thread.start();
				threads.add(thread);
			}
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}

}
