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
		if(length+3 > ChunkServer.ChunkSize){ //+3 because of metadata
			return ClientFS.FSReturnVals.RecordTooLong;
		}
		
		//verify with master
		if(!master.VerifyFileHandle(ofh.filename)){
			return ClientFS.FSReturnVals.BadHandle;
		}	
		
		//Get chunk handle from master
		String chunkHandle = master.GetHandleForAppend(ofh.filename, length);
		int offset = master.getEndOffset(ofh.filename);
		
		// Write the length of the record (2 bytes) right before the record itself	
		master.chunkserver.writeChunk(chunkHandle, ByteBuffer.allocate(2).putShort(length).array(), offset);
		offset += 2;
		// Write dirty byte
		master.chunkserver.writeChunk(chunkHandle, ByteBuffer.allocate(1).putShort((short) 1).array(), offset);
		offset += 1;
		// Write data
		master.chunkserver.writeChunk(chunkHandle, payload, offset);
		
		// Populate RID
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
		
		if(length == null){
			return ClientFS.FSReturnVals.RecDoesNotExist;
		}
		
		//Convert byte[] to int for chunk server
		ByteBuffer buffer = ByteBuffer.wrap(length);
		int len =  buffer.getInt();
		
		//Read at offset 3 (because of metadata) to get data
		byte[] payload = master.chunkserver.readChunk(chunkHandle, 3, len);
		
		//Populate rec
		rec.setPayload(payload);
		RID r = new RID();
		r.chunkHandle = chunkHandle;
		r.chunkOffset = 0; //points back to beginning of metadata 
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
		
		if(!master.VerifyFileHandle(ofh.filename)){
			return ClientFS.FSReturnVals.BadHandle;
		}
		
		//Get chunk handle from master
		String chunkHandle = master.GetLastChunkHandleOfAFile(ofh.filename);
		
		int offsetToRead = 0;
		int currentOffset = 0;
		int currentLength = 0;
		
		/* Note: we can expose helper functions on the chunkserver to minimize network calls */
		while(true){
			//Read 2 bytes at offsetToRead to determine how long the record is. 
			//Read after the length of the data to access next metadata.
			//Always read 2 bytes because that is the length 
			byte[] length = master.chunkserver.readChunk(chunkHandle, offsetToRead, 2);
			
			if(length == null){
				//read fails so we are at last record
				break;
			}
			
			//   1   2   3           n  
			//  ___ ___ ___         ___
			// |   |   |   | . . . |   | . . . <next metadata>
			// |___|___|___|       |___|
			//  length  dirty   <data>
			
			//save previous offset
			//+3 because of the length and dirty byte
			currentOffset = offsetToRead+3;
			//Convert byte[] to integer
			ByteBuffer buffer = ByteBuffer.wrap(length);
			currentLength =  buffer.getInt();
			
			//+1 for the dirty byte
			//+1 to go to the next metadata
			offsetToRead = currentOffset + currentLength;
		}
		
		//out of while loop means read fail, thus currentOffset has the last record
		//read at current offset witht he length provided by that offset
		//+1 because of the dirty byte
		byte[] payload = master.chunkserver.readChunk(chunkHandle, currentOffset, currentLength);
		
		//Populate rec
		rec.setPayload(payload);
		RID r = new RID();
		r.chunkHandle = chunkHandle;
		r.chunkOffset = currentOffset-3; //-3 to point back to the beginning of the metadata
		rec.setRID(r);
		
		return ClientFS.FSReturnVals.Success;
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
		
		if(!master.VerifyFileHandle(ofh.filename)){
			return ClientFS.FSReturnVals.BadHandle;
		}
		
		//check pivot
		// Skip deleted (invalidated) records
		
		String chunkHandle = pivot.chunkHandle;
		int offset = pivot.chunkOffset;
		
		byte[] length = master.chunkserver.readChunk(chunkHandle, offset, 2);
		
		
		if(length == null)
		//this record is the last in the chunk,
		//need next chunk handle and first record in that chunk
		chunkHandle = master.GetNextChunkHandle(ofh.filename, pivot.chunkHandle);
		
		//read length for the first chunk
		length = master.chunkserver.readChunk(chunkHandle, 0, 2);
		
		//Convert byte[] to integer
		ByteBuffer buffer = ByteBuffer.wrap(length);
		int len =  buffer.getInt();
		
		//read at offset 3 because of metadata
		byte[] data = master.chunkserver.readChunk(chunkHandle, 3, len);
		
		//populate rec
		RID r = new RID();
		r.chunkHandle = chunkHandle;
		r.chunkOffset = 0;
		rec.setRID(r);
		rec.setPayload(data);

		return ClientFS.FSReturnVals.Success;
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
