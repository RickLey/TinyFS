package com.master;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;

import com.chunkserver.ChunkServer;
import com.client.FileHandle;
public class Master {
	
	
	ArrayList<String> namespace; //Ordered list of FileHandles(String)
	HashMap<String, ArrayList<String>> chunkLists;	//Map from FileHandle to list of chunkHandle's
	HashMap<String, String> chunkLocations; //Map from chunkHandle to chunkServer IPs
	HashMap<String, Integer> remainingChunkSpace; // How much space is left in last chunk of a file
	
	/*ArrayList<Socket> chunkserverConnections;
	int currChunkserver;*/
	
	ChunkServer chunkserver;
	//Locks
	
	
	
	public Master(int portNumber, String hostname)
	{
		namespace = new ArrayList<String>();
		chunkLists = new HashMap<String, ArrayList<String>>();
		chunkLocations = new HashMap<String, String>();
		//currChunkserver = 0;
		//Write networking
		
		chunkserver = new ChunkServer();
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
	
	
	public static void main(String[] args) {
		new Master(1234, "localhost");

	}

}
