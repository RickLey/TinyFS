package com.client;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import com.chunkserver.ChunkServer;
import com.interfaces.ClientInterface;

/**
 * implementation of interfaces at the client side
 * @author Shahram Ghandeharizadeh
 *
 */
public class Client implements ClientInterface {
	
	private Socket socket;
	
	
	public static byte[] intToBytes(int input)
	{
		ByteBuffer dbuf = ByteBuffer.allocate(4);
		dbuf.putInt(input);
		return dbuf.array();
	}
	/**
	 * Initialize the client
	 */
	public Client(){

	}
	
	/**
	 * Create a chunk at the chunk server from the client side.
	 */
	public String createChunk() {
		try {
			socket = new Socket("localhost", 8080);
			InputStream is = socket.getInputStream();
			OutputStream os = socket.getOutputStream();
			os.write(1);
			int handleLength = ChunkServer.readInt(is);
			String handle = new String(ChunkServer.readAmount(is, handleLength));
			System.out.println("Client received handle " + handle);
			return handle;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * Write a chunk at the chunk server from the client side.
	 */
	public boolean writeChunk(String ChunkHandle, byte[] payload, int offset) {
		// Read the first four bytes to get the size then keep reading until you get 1k total
		if(offset + payload.length > ChunkServer.ChunkSize){
			System.out.println("The chunk write should be within the range of the file, invalide chunk write!");
			return false;
		}
		OutputStream os;
		InputStream is;
		try {
			socket = new Socket("localhost", 8080);
			os = socket.getOutputStream();
			is = socket.getInputStream();
			os.write(2);
			
			ByteBuffer dbuf = ByteBuffer.allocate(4);
			dbuf.putInt(ChunkHandle.length());
			os.write(dbuf.array());
			
			//os.write(intToBytes(ChunkHandle.length()));
			os.write(ChunkHandle.getBytes());
			os.write(intToBytes(payload.length));
			os.write(payload);
			os.write(intToBytes(offset));
			System.out.println("Completed Write chunk");
			return is.read() == 1;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	/**
	 * Read a chunk at the chunk server from the client side.
	 */
	public byte[] readChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		if(NumberOfBytes + offset > ChunkServer.ChunkSize){
			System.out.println("The chunk read should be within the range of the file, invalide chunk read!");
			return null;
		}
		return null;
	}

	


}
