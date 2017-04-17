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
		
		//Get chunk handle and offset from master
		String chunkHandle = master.GetHandleForAppend(ofh.filename, length);
		int offset = master.getEndOffset(ofh.filename);
		
		// Write the length of the record (2 bytes) right before the record itself	
		if(!master.chunkserver.writeChunk(chunkHandle, ByteBuffer.allocate(2).putShort(length).array(), offset)){
			return ClientFS.FSReturnVals.Fail;
		}
		// Write dirty byte
		if(!master.chunkserver.writeChunk(chunkHandle, ByteBuffer.allocate(1).putShort((short)1).array(), offset+2)){
			return ClientFS.FSReturnVals.Fail;
		}
		// Write data
		if(!master.chunkserver.writeChunk(chunkHandle, payload, offset+3)){
			return ClientFS.FSReturnVals.Fail;
		}
		
		// Populate RID
		RecordID = new RID();
		RecordID.chunkHandle = chunkHandle;
		RecordID.chunkOffset = offset;
		
		// Calls writeChunk with chunk handle, offset, payload. Must write
		// The length of the record (2 bytes) right before the record itself
		// And a byte to indicate (in)valid
		// Populates RID
		// Returns returnval
		
		
		
		
		return null;
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
		
		short dirtybyte = 1;
		int len = 0;
		int offset = 0;
		while(true){
			//Read the first 2 bytes to determine how long the record is and update offset
			byte[] length = master.chunkserver.readChunk(chunkHandle, offset, 2);
			offset += 2;
			
			if(length == null){
				return ClientFS.FSReturnVals.RecDoesNotExist;
			}
			
			// Read 1 byte at offset 2 to get dirty byte and update offset
			byte[] dirty = master.chunkserver.readChunk(chunkHandle, offset, 1);
			offset += 1;
			
			
			dirtybyte = ByteBuffer.wrap(dirty).getShort();
			len = ByteBuffer.wrap(length).getInt();

			if(dirtybyte == 0){
				//If dirtybyte is invalid, update offset for next read
				offset += len;
			} else {
				// Else break from the while loop
				break;
			}
		}
		
		// Exit while loop, thus we read chunk at offset for len number of bytes
		byte[] payload = master.chunkserver.readChunk(chunkHandle, offset, len);
		
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
			
			// Read dirty
			byte[] dirty = master.chunkserver.readChunk(chunkHandle, offsetToRead+2, 1);
			
			short dirtybyte = ByteBuffer.wrap(dirty).getShort();
			
			if(dirtybyte == 1){
				//save previous offset
				//+3 because of the length and dirty byte
				//Only update current offset if there is a valid record
				currentOffset = offsetToRead+3;		
			}
			//   1   2   3           n  
			//  ___ ___ ___         ___
			// |   |   |   | . . . |   | . . . <next metadata>
			// |___|___|___|       |___|
			//  length  dirty   <data>
			
			currentLength = ByteBuffer.wrap(length).getInt();
			//+1 for the dirty byte
			//+1 to go to the next metadata
			offsetToRead = currentLength + 3;
		}
		
		//out of while loop means read fail, thus currentOffset has the last valid record
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
		
		// Get chunk info from pivot
		String chunkHandle = pivot.chunkHandle;
		int offset = pivot.chunkOffset;
		
		//get length of the current to up
		byte[] length = master.chunkserver.readChunk(chunkHandle, offset, 2);
		int len = ByteBuffer.wrap(length).getInt();
		
		// update offset with it
		offset += len+3; //+3 because of metadata
		
		//start reading at next offset, we need to check if the next is valid,
		//if not, we need to keep reading
		while(true){
			//get length of the current
			length = master.chunkserver.readChunk(chunkHandle, offset, 2);
			
			if(length == null){
				//this record is the last record in the chunk, thus
				//thus, we need a the chunk handle of the next chunk
				chunkHandle = master.GetNextChunkHandle(ofh.filename, pivot.chunkHandle);
				//reset offset
				offset = 0;
				//we read again
				length = master.chunkserver.readChunk(chunkHandle, offset, 2);
			}
			len = ByteBuffer.wrap(length).getInt();

			byte[] dirty = master.chunkserver.readChunk(chunkHandle, offset+2, 1);
			
			short dirtybyte = ByteBuffer.wrap(dirty).getShort();
			
			if(dirtybyte == 1){
				//this record is good
				break;
			} else{
				//update offset for next read
				offset += len+3;
			}
		}

		//read len number of bytes at offset. Both len and offset are updated above
		byte[] data = master.chunkserver.readChunk(chunkHandle, offset, len);
		
		//populate rec
		RID r = new RID();
		r.chunkHandle = chunkHandle;
		r.chunkOffset = offset;
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
		
		if(!master.VerifyFileHandle(ofh.filename)){
			return ClientFS.FSReturnVals.BadHandle;
		}
		
		String chunkHandle = pivot.chunkHandle;
		int offset = pivot.chunkOffset;
		
		//now we will read up to the given offset by keeping track of the previous
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
				//read fails so we are at the beginning of the file
				//we need the chunk handle for the previous chunk
				chunkHandle = master.GetPreviousChunkHandle(ofh.filename, chunkHandle);
				
			}
			
			// Read dirty
			byte[] dirty = master.chunkserver.readChunk(chunkHandle, offsetToRead+2, 1);
			
			short dirtybyte = ByteBuffer.wrap(dirty).getShort();
			
			if(dirtybyte == 1){
				//save previous offset
				//+3 because of the length and dirty byte
				//Only update current offset if there is a valid record
				currentOffset = offsetToRead+3;		
			}
			//   1   2   3           n  
			//  ___ ___ ___         ___
			// |   |   |   | . . . |   | . . . <next metadata>
			// |___|___|___|       |___|
			//  length  dirty   <data>
			
			currentLength = ByteBuffer.wrap(length).getInt();
			//+1 for the dirty byte
			//+1 to go to the next metadata
			offsetToRead = currentLength + 3;
		}
		
		
		
		//return null;
	}

}
