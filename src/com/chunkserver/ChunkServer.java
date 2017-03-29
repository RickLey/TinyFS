package com.chunkserver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;

import com.client.Client;
import com.interfaces.ChunkServerInterface;

/**
 * implementation of interfaces at the chunkserver side
 * @author Shahram Ghandeharizadeh
 *
 */

public class ChunkServer implements ChunkServerInterface {
	final static String filePath = "C:\\Users\\fley\\Documents\\TinyFS\\csci485Disk\\";	//or C:\\newfile.txt
	public static long counter = 0;
	
	public static int readInt(InputStream is)
	{
		byte[] buffer = readAmount(is, 4);
		return ByteBuffer.wrap(buffer).getInt();
	}
	
	public static String readString (InputStream is, int length)
	{
		byte[] buffer = readAmount(is, length);
		return new String(buffer);
	}
	
	public static byte[] readAmount(InputStream is, int amount)
	{
		int readCount = 0;
		byte[] buffer = new byte[amount];
		while(readCount < amount)
		{
			try {
				readCount += is.read(buffer, readCount, amount - readCount);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return buffer;
	}
	
	/**
	 * Initialize the chunk server
	 */
	public ChunkServer(){
		try {
			ServerSocket listener = new ServerSocket();
			listener.bind(new InetSocketAddress("localhost", 8080));
			while(true)
			{
				Socket socket = listener.accept();
				InputStream is = socket.getInputStream();
				OutputStream os = socket.getOutputStream();
				int opId = is.read(); // Make sure that the method blocks until data is available.
				System.out.println("ChunkServer");
				System.out.println(opId);
				if(opId == 1)
				{
					String handle = createChunk();
					os.write(Client.intToBytes(handle.length()));
					os.write(handle.getBytes());
					socket.close();
				}
				else if(opId == 2)
				{
					int strLen = readInt(is);
					System.out.println(strLen);
					String handle = readString(is, strLen);
					System.out.println(handle);
					
					int payloadLen = readInt(is);
					System.out.println(payloadLen);
					byte[] payload = readAmount(is, payloadLen);
					System.out.println(payload[3]);
					
					int offset = readInt(is);
					System.out.println(offset);
					
					boolean result = writeChunk(handle, payload, offset);
					
					if(result)
						os.write(1);
					else
						os.write(0);
					
					socket.close();
					
				}
				else if(opId == 3)
				{
					int strLen = readInt(is);
					System.out.println(strLen);
					String handle = readString(is, strLen);
					System.out.println(handle);
					
					int offset = readInt(is);
					
					int numberOfBytes = readInt(is);
					System.out.println(offset);
					System.out.println(numberOfBytes);
					
					byte[] result = readChunk(handle, offset, numberOfBytes);
					
					os.write(Client.intToBytes(result.length));
					os.write(result);
					socket.close();
				}
				else
				{
					
				}
				//socket.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Each chunk corresponds to a file.
	 * Return the chunk handle of the last chunk in the file.
	 */
	public String createChunk() {
		String filename = Long.toString(counter) + ".chunk";
		Path path = Paths.get(filename);
		while(Files.exists(path))
		{
			counter += 1;
		}
		File file = new File(filePath + filename);
		counter += 1;
		
		return filename;
	}
	
	/**
	 * Write the byte array to the chunk at the specified offset
	 * The byte array size should be no greater than 4KB
	 */
	public boolean writeChunk(String ChunkHandle, byte[] payload, int offset) {
		try (RandomAccessFile file = new RandomAccessFile(filePath + ChunkHandle, "rw")) {
			if(payload.length + offset > 4* 1024)
			{
				return false;
			}
			file.seek(offset);
			file.write(payload, 0, payload.length);
			file.close();
		} catch (FileNotFoundException e) {
			System.out.println("Invalid chunk name " + ChunkHandle);
			return false;
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
		// Open the random access file
		// Loop over it from offset to offset + payload.length setting each byte to be equal. Memcopy function?
		// What if it goes past the end? Or greater than 4KB
	}
	
	/**
	 * read the chunk at the specific offset
	 */
	public byte[] readChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		try (RandomAccessFile file = new RandomAccessFile(filePath + ChunkHandle, "r")){
			byte[] data = new byte[NumberOfBytes];
			file.seek(offset);
			file.read(data, 0, NumberOfBytes);
			file.close();
			return data;
		} catch (FileNotFoundException e) {
			System.out.println("Invalid chunk name" + ChunkHandle);
			return null;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		// Open the random access file
		// Allocate array that is Number of Bytes
		// start at offset and go to offset + NumberOfBytes setting values in the array
		// What if it goes past the end of the chunk or is greater than 4KB?
	}
	
	
	public static void main(String args[])
	{
		ChunkServer cs = new ChunkServer();
	}

}

