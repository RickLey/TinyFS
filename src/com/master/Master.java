package com.master;

public class Master {

	public static int PORT = 1234;
	public static String HOST = "localhost";

	public static int CREATE_DIR_CMD = 101;
	public static int DELETE_DIR_CMD = 102;
	public static int RENAME_DIR_CMD = 103;
	public static int LIST_DIR_CMD = 104;
	public static int CREATE_FILE_CMD = 105;
	public static int DELETE_FILE_CMD = 106;
	public static int OPEN_FILE_CMD = 107;
	public static int CLOSE_FILE_CMD = 108;

	//Ordered list of FileHandles(String)
	//Map from FileHandle to list of chunkHandle's
	//Map from chunkHandle to chunkServer
	//Locks
	
	
	
	//Wirte networking
	
	public Master(int portNumber, String hostname)
	{
		
	}
	
	//Verify file handle and chunk handle
	
	boolean VerifyFileHandleAndChunkHandle(String fileHandle, String chunkHandle){
		
		
		
		
		return true;
	}
	
	/*
	 * Gets
	 */
	String GetNextChunkHandle(String ChunkHandle){
		
		
		
		return "";
	}
	
	String GetPreviousChunkHandle(String ChunkHandle){
		
		
		return "";
	}
	
	String GetLastChunkHandleOfAFile(String FileHandle){
		
		
		return "";
	}
	
	String GetFirstChunkHandleForFile(String FileHandle){
		
		
		return "";
	}
	
	String GetHandleHandleForAppend(String FileHandle){
		
		
		return "";
	}
	
	
	public static void main(String[] args) {
		new Master(PORT, HOST);

	}

}
