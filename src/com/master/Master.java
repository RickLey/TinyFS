package com.master;

import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

import com.chunkserver.ChunkServer;
import com.client.FileHandle;
public class Master {
	
	
	ArrayList<String> namespace; //Ordered list of FileHandles(String)
	HashMap<FileHandle, ArrayList<String>> chunkLists;	//Map from FileHandle to list of chunkHandle's
	HashMap<String, String> chunkLocations; //Map from chunkHandle to chunkServer IPs
	
	//Locks
	
	//Write networking
	
	public Master(int portNumber, String hostname)
	{
		namespace = new ArrayList<String>();
		chunkLists = new HashMap<FileHandle, ArrayList<String>>();
		chunkLocations = new HashMap<String, String>();
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
