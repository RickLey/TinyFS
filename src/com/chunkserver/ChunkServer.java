package com.chunkserver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


import com.interfaces.ChunkServerInterface;

/**
 * implementation of interfaces at the chunkserver side
 * @author Shahram Ghandeharizadeh
 *
 */

public class ChunkServer implements ChunkServerInterface {
	final static String filePath = "C:\\Users\\fley\\Documents\\TinyFS\\csci485Disk\\";	//or C:\\newfile.txt
	public static long counter = 0;
	
	/**
	 * Initialize the chunk server
	 */
	public ChunkServer(){
		System.out.println("Constructor of ChunkServer is invoked:  Part 1 of TinyFS must implement the body of this method.");
		System.out.println("It does nothing for now.\n");
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
	
	

}
