package com.client;

import java.nio.ByteBuffer;

import com.chunkserver.ChunkServer;
import com.client.ClientFS.FSReturnVals;
import com.master.Master;

public class ClientRec {

	//Create a master instance!
	//Passing (1, "") in arguments for part one ONLY. that will change with networking.
	Master master = new Master(1, "");
	
	// CHANGE TO master.chunkServer();
	ChunkServer cs = new ChunkServer();
	
	/**
	 * Appends a record to the open file as specified by ofh Returns BadHandle
	 * if ofh is invalid Returns BadRecID if the specified RID is not null
	 * Returns RecordTooLong if the size of payload exceeds chunksize RID is
	 * null if AppendRecord fails
	 *
	 * Example usage: AppendRecord(FH1, obama, RecID1)
	 */	
	public FSReturnVals AppendRecord(FileHandle ofh, byte[] payload, RID RecordID) {
				
		if(RecordID != null){
			return ClientFS.FSReturnVals.BadRecID;
		}
		
		//Check length of the payload with the chunk size
		short length = (short) payload.length;
		if(length > ChunkServer.ChunkSize){
			return ClientFS.FSReturnVals.RecordTooLong;
		}
		
		//Check file handle with master
		//String chunkHandle = master.validateFileHandle();
		

		/*
		 *  Master validates existence of file, looks up entry in map to chunk lists
		 *  Checks remaining space in last chunk (initialized to zero for a new file).
		 *  If chunk does not have
		 *  space to hold the whole record + metadata (size of record), creates a new chunk. 
		 *  Otherwise, returns
		 *  the last in the list 
		 *  (The master keeps an integer for remaining space on the last chunk and 
		 *  decrements it by the size of the payload. Resets it to max size upon 
		 *  new chunk creation.
		 *  Master must also send offset into chunk
		 */
		
		// Master returns chunk handle of last chunk
		
		// Calls writeChunk with chunk handle, offset, payload. Must write
		// The length of the record (2 bytes) right before the record itself
		
		
		
		//cs.writeChunk(ofh, );
		
		
		// And a byte to indicate (in)valid
		// Populates RID
		// Returns returnval
		
		
		
		
		return null;
	}

	/**
	 * Deletes the specified record by RecordID from the open file specified by
	 * ofh Returns BadHandle if ofh is invalid Returns BadRecID if the specified
	 * RID is not valid Returns RecDoesNotExist if the record specified by
	 * RecordID does not exist.
	 *
	 * Example usage: DeleteRecord(FH1, RecID1)
	 */
	public FSReturnVals DeleteRecord(FileHandle ofh, RID RecordID) {
		// Go to master to get the chunk of the record
		// Write a 0 to the valid byte
		return null;
	}

	/**
	 * Reads the first record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadFirstRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadFirstRecord(FileHandle ofh, TinyRec rec){
		// Call master
		/*
		 * Master returns first chunk handle for that file handle
		 */
		
		// Call read chunk. Read the first four bytes to determine how long the record is
		// Read read that much more into the chunk
		// Note: 
		return null;
	}

	/**
	 * Reads the last record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadLastRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadLastRecord(FileHandle ofh, TinyRec rec){
		/* Note: we can expose helper functions on the chunkserver to minimize network calls */
		/* Go to last chunk listed for the given file handle. Master returns chunk handle */
		
		// scan through the 
		// chunk until you get to the last record and return that.
		// Scan by reading size of record. Try reading at offset after that record.
		// If read fails, read the record at the offset I'm currently at, otherwise, 
		// move forward
		return null;
	}

	/**
	 * Reads the next record after the specified pivot of the file specified by
	 * ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadFirstRecord(FH1, tinyRec1) 2. ReadNextRecord(FH1,
	 * rec1, tinyRec2) 3. ReadNextRecord(FH1, rec2, tinyRec3)
	 */
	public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, TinyRec rec){
		/* Master validates file handle and chunk handle
		 * Master exposes API get next chunk for file. */
		
		// Get the chunk for this RID
		// Try to read after this record
		// If read succeeds, return
		// Else, ask master for next chunk handle
		// Read top record of that chunk
		// Finally, blend record
		
		// Skip deleted records
		return null;
	}

	/**
	 * Reads the previous record after the specified pivot of the file specified
	 * by ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadLastRecord(FH1, tinyRec1) 2. ReadPrevRecord(FH1,
	 * recn-1, tinyRec2) 3. ReadPrevRecord(FH1, recn-2, tinyRec3)
	 */
	public FSReturnVals ReadPrevRecord(FileHandle ofh, RID pivot, TinyRec rec){
		/* Master needs to expose a get previous chunk method */
		
		// Can determine we need the previous chunk by seeing the offset in RID is
		// 0. Would be helpful to have a read_last_record_of_chunk to use for this
		// and ReadLastRecord
		
		// Works basically the same as get last record except the EOF in that case
		// is now the offset of the current record, and we are not necessarily in the
		// last chunk of the whole file (instead we use the current chunk in RID)
		
		// Skip deleted records
		return null;
	}

}
