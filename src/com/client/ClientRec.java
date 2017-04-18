package com.client;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import com.chunkserver.ChunkServer;
import com.client.ClientFS.FSReturnVals;
import com.master.Master;

public class ClientRec {

	
	private Socket mSocket;
	private DataOutputStream mOut;
	private DataInputStream mIn;
	
	private Client client;

	//Create a master instance!
	//Passing (1, "") in arguments for part one ONLY. that will change with networking.
	Master master = new Master(new Socket());
	private int csport = 0;
	private String cshostname = "";
	
	public ClientRec(){
		//change the host and port number for master
		try {
			mSocket = new Socket("127.0.0.1", 1234);
			mOut = new DataOutputStream(mSocket.getOutputStream());
			mIn = new DataInputStream(mSocket.getInputStream());

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
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
		
		if(!verifyFileHandleWithMaster(ofh.filename)){
			return ClientFS.FSReturnVals.BadHandle;
		}
		/*
		//verify with master
		if(!master.VerifyFileHandle(ofh.filename)){
			return ClientFS.FSReturnVals.BadHandle;
		}	
		*/
		
		//Get chunk handle and offset from master
		//String chunkHandle = master.GetHandleForAppend(ofh.filename, length);
		String chunkHandle = "";
		try {
			//Write packet size
			//mOut.WriteInt(4); //????
			//Write command
			mOut.writeInt(Master.GET_HANDLE_FOR_APPEND_CMD);
			mOut.writeInt(payload.length);
			mOut.writeInt(ofh.filename.length());
			mOut.writeBytes(ofh.filename);
			
			int packetSize = mIn.readInt();
			int size = mIn.readInt();
			chunkHandle = readString(size);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		//TODO: NEED COMMAND FOR THIS
		int offset = master.getEndOffset(ofh.filename);
		
		/*
		if(!master.chunkserver.writeChunk(chunkHandle, ByteBuffer.allocate(2).putShort(length).array(), offset)){
			return ClientFS.FSReturnVals.Fail;
		}
		*/
		// Write the length of the record (2 bytes) right before the record itself	
		if(!client.writeChunk(chunkHandle, ByteBuffer.allocate(2).putShort(length).array(), offset)){
			return ClientFS.FSReturnVals.Fail;
		}
		/*
		// Write dirty byte
		if(!master.chunkserver.writeChunk(chunkHandle, ByteBuffer.allocate(1).putShort((short)1).array(), offset+2)){
			return ClientFS.FSReturnVals.Fail;
		}
		*/
		// Write dirty byte
		if(!client.writeChunk(chunkHandle, ByteBuffer.allocate(1).putShort((short)1).array(), offset+2)){
			return ClientFS.FSReturnVals.Fail;
		}		
		
		/*
		// Write data
		if(!master.chunkserver.writeChunk(chunkHandle, payload, offset+3)){
			return ClientFS.FSReturnVals.Fail;
		}
		*/
		// Write data
		if(!client.writeChunk(chunkHandle, payload, offset+3)){
			return ClientFS.FSReturnVals.Fail;
		}
		
		// Populate RID
		RecordID = new RID();
		RecordID.chunkHandle = chunkHandle;
		RecordID.chunkOffset = offset;
		
		
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
	
	//NETWORKNNG ADDED
	public FSReturnVals DeleteRecord(FileHandle ofh, RID RecordID) {

		if(!verifyFileHandleWithMaster(ofh.filename)){
			return ClientFS.FSReturnVals.BadHandle;
		}
		/*
		if(!master.VerifyFileHandle(ofh.filename)){
			return ClientFS.FSReturnVals.BadHandle;
		}
		*/
		
		if(RecordID == null){
			return ClientFS.FSReturnVals.BadRecID;
		}
		
		
		String chunkHandle = RecordID.chunkHandle;
		int offset = RecordID.chunkOffset;
		

		ConnectToChunkServer(chunkHandle);
		
		if(!client.writeChunk(chunkHandle, ByteBuffer.allocate(1).putShort((short) 0).array(), offset+2)){
			return ClientFS.FSReturnVals.Fail;
		}
		
		/*
		if(!master.chunkserver.writeChunk(chunkHandle, ByteBuffer.allocate(1).putShort((short) 0).array(), offset+2)){ //+2 because of metadata
			return ClientFS.FSReturnVals.Fail;
		}
		*/
		return ClientFS.FSReturnVals.Success;
	}

	/**
	 * Reads the first record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadFirstRecord(FH1, tinyRec)
	 */
	
	//NETWORKIGN ADDED, NO OPTIMIZATION
	public FSReturnVals ReadFirstRecord(FileHandle ofh, TinyRec rec){

		if(!verifyFileHandleWithMaster(ofh.filename)){
			return ClientFS.FSReturnVals.BadHandle;
		}
		/*
		//Verify handle with master
		if(!master.VerifyFileHandle(ofh.filename)){
			return ClientFS.FSReturnVals.BadHandle;
		}
		*/
		
		// Get first chunk handle from master
		String chunkHandle = "";
		try {
			//Write packet size
			//mOut.WriteInt(4); //????
			//Write command
			mOut.writeInt(Master.GET_FIRST_CHUNK_HANDLE_FOR_FILE_CMD);
			mOut.writeInt(ofh.filename.length());
			mOut.writeBytes(ofh.filename);
		
			int size = mIn.readInt();
			chunkHandle = readString(size);
		} catch (IOException e) {
			e.printStackTrace();
		}
		/*
		//Get chunk handle from master
		String chunkHandle = master.GetFirstChunkHandleForFile(ofh.filename);
		*/
		
		ConnectToChunkServer(chunkHandle);
		
		short dirtybyte = 1;
		int len = 0;
		int offset = 0;
		while(true){
			//Read the first 2 bytes to determine how long the record is and update offset
			//byte[] length = master.chunkserver.readChunk(chunkHandle, offset, 2);
			byte[] length = client.readChunk(chunkHandle, offset, 2);
			
			offset += 2;
			
			if(length == null){
				return ClientFS.FSReturnVals.RecDoesNotExist;
			}
			
			// Read 1 byte at offset 2 to get dirty byte and update offset
			//byte[] dirty = master.chunkserver.readChunk(chunkHandle, offset, 1);
			byte[] valid = client.readChunk(chunkHandle, offset, 1);
			offset += 1;
			
			
			dirtybyte = ByteBuffer.wrap(valid).getShort();
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
		//byte[] payload = master.chunkserver.readChunk(chunkHandle, offset, len);
		byte[] payload = client.readChunk(chunkHandle, offset, len);
		
		//Populate rec
		rec.setPayload(payload);
		RID r = new RID();
		r.chunkHandle = chunkHandle;
		r.chunkOffset = offset-3; //points back to beginning of metadata 
		rec.setRID(r);
		
		return ClientFS.FSReturnVals.Success;
	}

	/**
	 * Reads the last record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadLastRecord(FH1, tinyRec)
	 */
	//NETWORKING ADDED, NO OPTIMIZATION
	public FSReturnVals ReadLastRecord(FileHandle ofh, TinyRec rec){
		
		if(!verifyFileHandleWithMaster(ofh.filename)){
			return ClientFS.FSReturnVals.BadHandle;
		}
		/*
		//Verify handle with master
		if(!master.VerifyFileHandle(ofh.filename)){
			return ClientFS.FSReturnVals.BadHandle;
		}
		*/
		// Get first chunk handle from master
		String chunkHandle = "";
		try {
			//Write packet size
			//mOut.WriteInt(4); //????
			//Write command
			mOut.writeInt(Master.GET_LAST_CHUNK_HANDLE_FOR_FILE_CMD);
			mOut.writeInt(ofh.filename.length());
			mOut.writeBytes(ofh.filename);
		
			int size = mIn.readInt();
			chunkHandle = readString(size);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//Get chunk handle from master
		//String chunkHandle = master.GetLastChunkHandleOfAFile(ofh.filename);
		
		int offsetToRead = 0;
		int currentOffset = 0;
		int currentLength = 0;
		
		/* Note: we can expose helper functions on the chunkserver to minimize network calls */
		while(true){
			//Read 2 bytes at offsetToRead to determine how long the record is. 
			//Read after the length of the data to access next metadata.
			//Always read 2 bytes because that is the length 
			//byte[] length = master.chunkserver.readChunk(chunkHandle, offsetToRead, 2);
			byte[]length = client.readChunk(chunkHandle, offsetToRead, 2);
			
			if(length == null){
				//read fails so we are at last record
				break;
			}
			
			// Read dirty
			//byte[] dirty = master.chunkserver.readChunk(chunkHandle, offsetToRead+2, 1);
			byte[] valid = client.readChunk(chunkHandle, offsetToRead+2, 1);
			
			short validbyte = ByteBuffer.wrap(valid).getShort();
			
			if(validbyte == 1){
				//save previous offset
				//+3 because of the length and dirty byte
				//Only update current offset if there is a valid record
				
				//TODO: keep track of preiouvs length as well
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
			offsetToRead += currentLength + 3;
		}
		
		//out of while loop means read fail, thus currentOffset has the last valid record
		//read at current offset witht he length provided by that offset
		//+1 because of the dirty byte
		//byte[] payload = master.chunkserver.readChunk(chunkHandle, currentOffset, currentLength);
		byte[] payload = client.readChunk(chunkHandle, currentOffset, currentLength);
		
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
	
	//NETWORKING ADDED, NO OPTIMIZATION
	public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, TinyRec rec){
		
		if(!verifyFileHandleWithMaster(ofh.filename)){
			return ClientFS.FSReturnVals.BadHandle;
		}
		/*
		//Verify handle with master
		if(!master.VerifyFileHandle(ofh.filename)){
			return ClientFS.FSReturnVals.BadHandle;
		}
		*/
		
		//check pivot
		// Skip deleted (invalidated) records
		
		// Get chunk info from pivot
		String chunkHandle = pivot.chunkHandle;
		int offset = pivot.chunkOffset;
		
		//get length of the current to up
		//byte[] length = master.chunkserver.readChunk(chunkHandle, offset, 2);
		byte[] length = client.readChunk(chunkHandle, offset, 2);
		
		int len = ByteBuffer.wrap(length).getInt();
		
		// update offset with it
		offset += len+3; //+3 because of metadata
		
		//start reading at next offset, we need to check if the next is valid,
		//if not, we need to keep reading
		while(true){
			//get length of the current
			//length = master.chunkserver.readChunk(chunkHandle, offset, 2);
			length = client.readChunk(chunkHandle, offset, 2);
			
			if(length == null){
				//this record is the last record in the chunk, thus
				//thus, we need a the chunk handle of the next chunk
				
				
				//chunkHandle = master.GetNextChunkHandle(ofh.filename, pivot.chunkHandle);
				try {
					//Write packet size
					//mOut.WriteInt(4); //????
					//Write command
					mOut.writeInt(Master.GET_NEXT_CHUNK_HANDLE_CMD);
					mOut.write(ofh.filename.length());
					mOut.writeInt(chunkHandle.length());
					
					mOut.writeBytes(ofh.filename);
					mOut.writeBytes(chunkHandle);
					
					int packetSize = mIn.readInt();
					int size = mIn.readInt();
					chunkHandle = readString(size);
					
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
				
				//reset offset
				offset = 0;
				//we read again
				//length = master.chunkserver.readChunk(chunkHandle, offset, 2);
				length = client.readChunk(chunkHandle, offset, 2);
			}
			len = ByteBuffer.wrap(length).getInt();

			//byte[] dirty = master.chunkserver.readChunk(chunkHandle, offset+2, 1);
			byte[] valid = client.readChunk(chunkHandle, offset+2, 1);
			
			short validbyte = ByteBuffer.wrap(valid).getShort();
			
			if(validbyte == 1){
				//this record is good
				break;
			} else{
				//update offset for next read
				offset += len+3;
			}
		}

		//read len number of bytes at offset. Both len and offset are updated above
		//byte[] data = master.chunkserver.readChunk(chunkHandle, offset, len);
		byte[] payload = client.readChunk(chunkHandle, offset, len);
		
		//populate rec
		RID r = new RID();
		r.chunkHandle = chunkHandle;
		r.chunkOffset = offset;
		rec.setRID(r);
		rec.setPayload(payload);

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
	//NETWORKING ADDED, NO OPTIMIATION
	public FSReturnVals ReadPrevRecord(FileHandle ofh, RID pivot, TinyRec rec){
		
		if(!verifyFileHandleWithMaster(ofh.filename)){
			return ClientFS.FSReturnVals.BadHandle;
		}
		
		/*
		//Verify handle with master
		if(!master.VerifyFileHandle(ofh.filename)){
			return ClientFS.FSReturnVals.BadHandle;
		}
		*/
		
		String chunkHandle = pivot.chunkHandle;
		int offset = pivot.chunkOffset;
		
		// If offset 0, it means we are in the first record of the chunk
		// Thus we need the handle and offset for the previous chunk 
		if(offset == 0){
			//chunkHandle = master.GetPreviousChunkHandle(ofh.filename, chunkHandle);
			
			try {
				//Write packet size
				//mOut.WriteInt(4); //????
				//Write command
				mOut.writeInt(Master.GET_PREVIOUS_CHUNK_HANDLE_CMD);
				mOut.write(ofh.filename.length());
				mOut.writeInt(chunkHandle.length());
				
				mOut.writeBytes(ofh.filename);
				mOut.writeBytes(chunkHandle);
				
				int packetSize = mIn.readInt();
				int size = mIn.readInt();
				chunkHandle = readString(size);
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
			
			RID r = new RID();
			byte[] payload =  this.ReadLastRecordInChunk(chunkHandle, r);

			rec.setRID(r);
			rec.setPayload(payload);
			
			return ClientFS.FSReturnVals.Success;
		}
		
		
		
		//now we will read up to the given offset by keeping track of the previous
		int offsetToRead = 0;
		int currentOffset = 0;
		int currentLength = 0;
		
		/* Note: we can expose helper functions on the chunkserver to minimize network calls */
		while(true){
			//Read 2 bytes at offsetToRead to determine how long the record is. 
			//Read after the length of the data to access next metadata.
			//Always read 2 bytes because that is the length 
			//byte[] length = master.chunkserver.readChunk(chunkHandle, offsetToRead, 2);
			byte[] length = client.readChunk(chunkHandle, offsetToRead, 2);
			
			/*
			if(length == null){
				break;
			}
			*/
			// Read dirty
			//byte[] dirty = master.chunkserver.readChunk(chunkHandle, offsetToRead+2, 1);
			byte[] valid = client.readChunk(chunkHandle, offsetToRead+2, 1);
			
			short validbyte = ByteBuffer.wrap(valid).getShort();
			
			if(validbyte == 1){
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
			
			if(offsetToRead == offset){
				//Reached goal offset
				break;
			}
			
		}
		
		//exit while loop means current offset has the previous record of length currentLength
		//byte[] payload = master.chunkserver.readChunk(chunkHandle, currentOffset, currentLength);
		byte[] payload = client.readChunk(chunkHandle, currentOffset, currentLength);
		
		//Populate rec
		rec.setPayload(payload);
		RID r = new RID();
		r.chunkHandle = chunkHandle;
		r.chunkOffset = currentOffset-3; //-3 to point back to the beginning of the metadata
		rec.setRID(r);
		
		return ClientFS.FSReturnVals.Success;
	}
	
	//NETOWRKING ADDED, NO OPTIMIZATION
	public byte[] ReadLastRecordInChunk(String chunkHandle, RID r){
		
		int offsetToRead = 0;
		int currentOffset = 0;
		int currentLength = 0;
		
		/* Note: we can expose helper functions on the chunkserver to minimize network calls */
		while(true){
			//Read 2 bytes at offsetToRead to determine how long the record is. 
			//Read after the length of the data to access next metadata.
			//Always read 2 bytes because that is the length 
			//byte[] length = master.chunkserver.readChunk(chunkHandle, offsetToRead, 2);
			byte[] length = client.readChunk(chunkHandle, offsetToRead, 2);
					
			if(length == null){
				//read fails so we are at last record
				break;
			}
			
			// Read dirty
			//byte[] dirty = master.chunkserver.readChunk(chunkHandle, offsetToRead+2, 1);
			byte[] valid = client.readChunk(chunkHandle, offsetToRead+2, 1);
			
			short validbyte = ByteBuffer.wrap(valid).getShort();
			
			if(validbyte == 1){
				//save previous offset
				//+3 because of the length and dirty byte
				//Only update current offset if there is a valid record
				
				//TODO: keep track of preiouvs length as well
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
			offsetToRead += currentLength + 3;
		}
		
		//out of while loop means read fail, thus currentOffset has the last valid record
		//read at current offset witht he length provided by that offset
		//+1 because of the dirty byte
		//byte[] payload = master.chunkserver.readChunk(chunkHandle, currentOffset, currentLength);
		byte[] payload = client.readChunk(chunkHandle, currentOffset, currentLength);
		
		//populate rec
		r.chunkHandle = chunkHandle;
		r.chunkOffset = currentOffset-3;
		return payload;
	}

	//TODO: REVIEW
	void ConnectToChunkServer(String chunkHandle){
		//Notify the server to get chunk server info
		try {
			//Write packet size
			//mOut.WriteInt(4); //????
			//Write command
			//mOut.writeInt(Master.);
			mOut.writeInt(chunkHandle.length());
			mOut.writeBytes(chunkHandle);
			mOut.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		byte[] host = null;
		Socket s = null;
		try {
			int size = mIn.readInt();
			int hostsize = mIn.readInt();
			mIn.read(host, 0, hostsize);
			int port = mIn.readInt();
			String hostname = new String(host);
			
			if(csport == port && cshostname == hostname){
				// already connected to this chunk server
				return;
			}
			
			this.csport = port;
			this.cshostname = hostname;
			client = new Client(cshostname, csport);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private String readString(int size) throws IOException {
		byte[] bytes = new byte[size];
		int bytesRead = 0;
		while (bytesRead < size) {
			bytesRead += mIn.read(bytes, bytesRead, size - bytesRead);
		}
		return new String(bytes);
	}
	
	private boolean verifyFileHandleWithMaster(String fileHandle){
		// Send file handle to master for verification
		boolean success = false;
		try {
			//Write packet size
			//mOut.WriteInt(4); //????
			//Write command
			mOut.writeInt(Master.VERIFY_FILE_HANDLE_CMD);
			mOut.writeInt(fileHandle.length());
			mOut.writeBytes(fileHandle);
			
			int size = mIn.readInt();
			success = mIn.readBoolean();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return success;
	}
}

