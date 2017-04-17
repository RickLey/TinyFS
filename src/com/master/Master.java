package com.master;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

import com.chunkserver.ChunkServer;
import com.client.FileHandle;
import com.client.ClientFS;
import com.client.ClientFS.FSReturnVals;
public class Master {

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

	//Ordered list of FileHandles(String)
	//Map from FileHandle to list of chunkHandle's
	//Map from chunkHandle to chunkServer
	//Locks
	
	ArrayList<String> namespace; //Ordered list of FileHandles(String)
	HashMap<String, ArrayList<String>> chunkLists;	//Map from FileHandle to list of chunkHandle's
	HashMap<String, String> chunkLocations; //Map from chunkHandle to chunkServer IPs
	HashMap<String, Integer> remainingChunkSpace; // How much space is left in last chunk of a file
	
	/*ArrayList<Socket> chunkserverConnections;
	int currChunkserver;*/
	
	ChunkServer chunkserver;
	//Locks
	
	
	
	public Master()
	{
		namespace = new ArrayList<String>();
		chunkLists = new HashMap<String, ArrayList<String>>();
		chunkLocations = new HashMap<String, String>();
		remainingChunkSpace = new HashMap<String, Integer>();
		namespace.add("/");
		//currChunkserver = 0;
		
		chunkserver = new ChunkServer();
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
		int index = namespace.indexOf(src + "/");
		if(index < 0)
		{
			return ClientFS.FSReturnVals.SrcDirNotExistent;
		}
		
		if(namespace.indexOf(NewName + "/") >= 0)
		{
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
		
		// Must sort at the end to maintain canonical order? -- shouldn't be necessary
		// as they will all still be clustered
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
		// Iterate through ordered list until there's a prefix that is not the target dir
		int index = namespace.indexOf(tgt + "/");
		if(index < 0)
		{
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

	
	boolean VerifyFileHandle(String fileHandle)
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
			return list.get(list.size() - 1);
		}
		else
		{
			// Call a chunkserver to create a chunk
			// Rotate the chunkserver to create new chunks
			/*Socket chunkserver = chunkserverConnections.get(currChunkserver 
					% chunkserverConnections.size());
			currChunkserver++;
			try {
				chunkserver.getOutputStream().write(ChunkServer.CreateChunkCMD);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			Read the string handle back
			*/
			
			String chunkHandle = chunkserver.createChunk();
			chunkLists.get(FileHandle).add(chunkHandle); // add the handle to the list for this file
			remainingChunkSpace.put(FileHandle, ChunkServer.ChunkSize - payloadSize); // reset the remaining space to be chunksize - payloadsize
			return chunkHandle;
		}
	}

	/*	CREATE_DIR_CMD Packet Layout
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

		byte[] srcBytes = new byte[srcSize];
		int bytesRead = 0;
		while (bytesRead < srcSize) {
			bytesRead += in.read(srcBytes, bytesRead, srcSize - bytesRead);
		}
		String src = new String(srcBytes);

		byte[] dirnameBytes = new byte[dirnameSize];
		bytesRead = 0;
		while (bytesRead < dirnameSize) {
			bytesRead += in.read(dirnameBytes, bytesRead, dirnameSize - bytesRead);
		}
		String dirname = new String(dirnameBytes);

		FSReturnVals returnVal = CreateDir(src, dirname);

		out.writeInt(8);
		out.writeInt(returnVal.getValue());
	}

	/*	LIST_DIR_CMD Packet Layout
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

		byte[] targetBytes = new byte[targetSize];
		int bytesRead = 0;
		while (bytesRead < targetSize) {
			bytesRead += in.read(targetBytes, bytesRead, targetSize - bytesRead);
		}
		String target = new String(targetBytes);

		String[] listings = ListDir(target);

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

	public static void main(String[] args) {
		Master master = new Master();

		try {
			ServerSocket server = new ServerSocket(Master.PORT);

			Socket connection = null;
			DataOutputStream out = null;
			DataInputStream in = null;

			while (true) {
				connection = server.accept();
				out = new DataOutputStream(new BufferedOutputStream(connection.getOutputStream()));
				in = new DataInputStream(new BufferedInputStream(connection.getInputStream()));

				while (!connection.isClosed()) {
					int packetSize = in.readInt();
					if (packetSize == -1) {
						break;
					}

					int command = in.readInt();
					switch (command) {
						case CREATE_DIR_CMD:
							master.handleCreateDirCmd(in, out);
							break;

						case DELETE_DIR_CMD:
							// TODO
							break;

						case RENAME_DIR_CMD:
							// TODO
							break;

						case LIST_DIR_CMD:
							master.handleListDirCmd(in, out);
							break;

						case CREATE_FILE_CMD:
							// TODO
							break;

						case DELETE_FILE_CMD:
							// TODO
							break;

						case OPEN_FILE_CMD:
							// TODO
							break;

						case CLOSE_FILE_CMD:
							// TODO
							break;
					}

					out.flush();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}

}
