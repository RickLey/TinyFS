package com.master;

public class Master {
	
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
		new Master(1234, "localhost");

	}

}
