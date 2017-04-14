package com.client;

import java.nio.ByteBuffer;

import com.chunkserver.ChunkServer;
import com.client.ClientFS.FSReturnVals;
import com.master.Master;

public class ClientRec {

	//Create a master instance!
	//Passing (1, "") in arguments for part one ONLY. that will change with networking.
	Master master = new Master(1, "");
	
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
		
		//verify with master
		if(!master.VerifyFileHandle(ofh.filename)){
			return ClientFS.FSReturnVals.BadHandle;
		}	
		
		//Get chunk handle from master
		String chunkHandle = master.GetHandleForAppend(ofh.filename, length);

		// Write the length of the record (2 bytes) right before the record itself	
		//Convert short to byte[] for chunk server
		ByteBuffer buffer = ByteBuffer.allocate(2);
		buffer.putShort(length);
		
		// Write the length NOT SURE ABOUT THE OFFSET BEING 0
		master.chunkserver.writeChunk(chunkHandle, buffer.array(), 0);
		
		// Write the dirty byte
		buffer = ByteBuffer.allocate(1);
		buffer.putShort((short) 1);
		master.chunkserver.writeChunk(chunkHandle, payload, 0);
		
		//write data
		master.chunkserver.writeChunk(chunkHandle, payload, 0);
		
		// Populates RID
		RecordID = new RID();
		RecordID.chunkHandle = chunkHandle;
		RecordID.chunkOffset = length;
		
		return ClientFS.FSReturnVals.Success;
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
		// Go to master to validate the chunk of the record
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

		//Verify handle with master
		if(!master.VerifyFileHandle(ofh.filename)){
			return ClientFS.FSReturnVals.BadHandle;
		}
		
		//Get chunk handle from master
		String chunkHandle = master.GetFirstChunkHandleForFile(ofh.filename);
		
		//Read the first 2 bytes to determine how long the record is
		byte[] length = master.chunkserver.readChunk(chunkHandle, 0, 2);
		
		if(length.length == 0){
			return ClientFS.FSReturnVals.RecDoesNotExist;
		}
		
		//Convert short to byte[] for chunk server
		ByteBuffer buffer = ByteBuffer.wrap(length);
		int len =  buffer.getInt();
		
		//Read at offset 2 (because previous read) for len number of bytes
		byte[] payload = master.chunkserver.readChunk(chunkHandle, 2, len);
		
		//Populate rec
		rec.setPayload(payload);
		RID r = new RID();
		r.chunkHandle = chunkHandle;
		r.chunkOffset = len+3; //+3 because of metadata
		rec.setRID(r);
		
		return ClientFS.FSReturnVals.Success;
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
		// Finally, return record
		
		// Skip deleted (invalidated) records
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
