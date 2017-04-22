package com.client;

import com.master.Master;

import java.io.*;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class ClientFS {

	// enum <-> int conversion as found at: http://codingexplained.com/coding/java/enum-to-integer-and-integer-to-enum
	public enum FSReturnVals {
		DirExists(0), // Returned by CreateDir when directory exists
		DirNotEmpty(1), //Returned when a non-empty directory is deleted
		SrcDirNotExistent(2), // Returned when source directory does not exist
		DestDirExists(3), // Returned when a destination directory exists
		FileExists(4), // Returned when a file exists
		FileDoesNotExist(5), // Returns when a file does not exist
		BadHandle(6), // Returned when the handle for an open file is not valid
		RecordTooLong(7), // Returned when a record size is larger than chunk size
		BadRecID(8), // The specified RID is not valid, used by DeleteRecord
		RecDoesNotExist(9), // The specified record does not exist, used by DeleteRecord
		NotImplemented(10), // Specific to CSCI 485 and its unit tests
		Success(11), //Returned when a method succeeds
		Fail(12); //Returned when a method fails

		private int value;

		FSReturnVals(int value) {
			this.value = value;
		}

		public int getValue() {
			return this.value;
		}

		private static Map<Integer, FSReturnVals> valueMap = new HashMap<Integer, FSReturnVals>();

		static {
			for (FSReturnVals returnVal : FSReturnVals.values()) {
				valueMap.put(returnVal.value, returnVal);
			}
		}

		public static FSReturnVals valueOf(int value) {
			return valueMap.get(value);
		}
	}

	private Socket socket;
	private DataOutputStream out;
	private DataInputStream in;

	public ClientFS() {
		try {
			socket = new Socket(Master.HOST, Master.PORT);
			out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
			in = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}

	/**
	 * Creates the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: CreateDir("/", "Shahram"), CreateDir("/Shahram/",
	 * "CSCI485"), CreateDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals CreateDir(String src, String dirname) {
		byte[] srcBytes = src.getBytes();
		byte[] dirnameBytes = dirname.getBytes();

		try {
			int outPacketSize = 16 + srcBytes.length + dirnameBytes.length;
			out.writeInt(outPacketSize);
			out.writeInt(Master.CREATE_DIR_CMD);
			out.writeInt(srcBytes.length);
			out.writeInt(dirnameBytes.length);
			out.write(srcBytes);
			out.write(dirnameBytes);
			out.flush();

			System.out.println("Waiting for response");
			int inPacketSize = in.readInt();
			int retValue = in.readInt();
			System.out.println("Received response");
			return FSReturnVals.valueOf(retValue);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return FSReturnVals.Fail;
	}

	/**
	 * Deletes the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: DeleteDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals DeleteDir(String src, String dirname) {
		byte[] srcBytes = src.getBytes();
		byte[] dirnameBytes = dirname.getBytes();

		try {
			int outPacketSize = 16 + srcBytes.length + dirnameBytes.length;
			out.writeInt(outPacketSize);
			out.writeInt(Master.DELETE_DIR_CMD);
			out.writeInt(srcBytes.length);
			out.writeInt(dirnameBytes.length);
			out.write(srcBytes);
			out.write(dirnameBytes);
			out.flush();

			int inPacketSize = in.readInt();
			int retValue = in.readInt();
			return FSReturnVals.valueOf(retValue);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return FSReturnVals.Fail;
	}

	/**
	 * Renames the specified src directory in the specified path to NewName
	 * Returns SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if a directory with NewName exists in the specified path
	 *
	 * Example usage: RenameDir("/Shahram/CSCI485", "/Shahram/CSCI550") changes
	 * "/Shahram/CSCI485" to "/Shahram/CSCI550"
	 */
	public FSReturnVals RenameDir(String src, String NewName) {
		byte[] srcBytes = src.getBytes();
		byte[] newnameBytes = NewName.getBytes();

		try {
			int outPacketSize = 16 + srcBytes.length + newnameBytes.length;
			out.writeInt(outPacketSize);
			out.writeInt(Master.RENAME_DIR_CMD);
			out.writeInt(srcBytes.length);
			out.writeInt(newnameBytes.length);
			out.write(srcBytes);
			out.write(newnameBytes);
			out.flush();

			int inPacketSize = in.readInt();
			int retValue = in.readInt();
			return FSReturnVals.valueOf(retValue);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return FSReturnVals.Fail;
	}

	/**
	 * Lists the content of the target directory Returns SrcDirNotExistent if
	 * the target directory does not exist Returns null if the target directory
	 * is empty
	 *
	 * Example usage: ListDir("/Shahram/CSCI485")
	 */
	public String[] ListDir(String tgt) {
		// Iterate through ordered list until there's a prefix that is not the target dir

		byte[] tgtBytes = tgt.getBytes();

		try {
			int outPacketSize = 12 + tgtBytes.length;
			out.writeInt(outPacketSize);
			out.writeInt(Master.LIST_DIR_CMD);
			out.writeInt(tgtBytes.length);
			out.write(tgtBytes);
			out.flush();

			int inPacketSize = in.readInt();

			if (inPacketSize == 4) {
				return null;
			}

			int numListings = in.readInt();
			int bytesRead = 0;
			String[] listings = new String[numListings];
			for (int i = 0 ; i < numListings ; i++) {
				int listingSize = in.readInt();
				byte[] listing = new byte[listingSize];
				bytesRead = 0;
				while (bytesRead < listingSize) {
					bytesRead += in.read(listing, bytesRead, listingSize - bytesRead);
				}
				listings[i] = new String(listing);
			}

			return listings;
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}

	/**
	 * Creates the specified filename in the target directory Returns
	 * SrcDirNotExistent if the target directory does not exist Returns
	 * FileExists if the specified filename exists in the specified directory
	 *
	 * Example usage: Createfile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals CreateFile(String tgtdir, String filename) {
		// Send message to master
		// Master will insert filename into ordered list
		
		// Files are handled the same as directories. All directories end with a /

		byte[] tgtdirBytes = tgtdir.getBytes();
		byte[] filenameBytes = filename.getBytes();

		try {
			int outPacketSize = 16 + tgtdirBytes.length + filenameBytes.length;
			out.writeInt(outPacketSize);
			out.writeInt(Master.CREATE_FILE_CMD);
			out.writeInt(tgtdirBytes.length);
			out.writeInt(filenameBytes.length);
			out.write(tgtdirBytes);
			out.write(filenameBytes);
			out.flush();

			int inPacketSize = in.readInt();
			int retValue = in.readInt();
			return FSReturnVals.valueOf(retValue);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return FSReturnVals.Fail;
	}

	/**
	 * Deletes the specified filename from the tgtdir Returns SrcDirNotExistent
	 * if the target directory does not exist Returns FileDoesNotExist if the
	 * specified filename is not-existent
	 *
	 * Example usage: DeleteFile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals DeleteFile(String tgtdir, String filename) {
		byte[] tgtdirBytes = tgtdir.getBytes();
		byte[] filenameBytes = filename.getBytes();

		try {
			int outPacketSize = 16 + tgtdirBytes.length + filenameBytes.length;
			out.writeInt(outPacketSize);
			out.writeInt(Master.DELETE_FILE_CMD);
			out.writeInt(tgtdirBytes.length);
			out.writeInt(filenameBytes.length);
			out.write(tgtdirBytes);
			out.write(filenameBytes);
			out.flush();

			int inPacketSize = in.readInt();
			int retValue = in.readInt();
			return FSReturnVals.valueOf(retValue);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return FSReturnVals.Fail;
	}

	/**
	 * Opens the file specified by the FilePath and populates the FileHandle
	 * Returns FileDoesNotExist if the specified filename by FilePath is
	 * not-existent
	 *
	 * Example usage: OpenFile("/Shahram/CSCI485/Lecture1/Intro.pptx", FH1)
	 */
	public FSReturnVals OpenFile(String FilePath, FileHandle ofh) {
		// Return the FilePath as a file handle. Vacuous.
		byte[] filepathBytes = FilePath.getBytes();

		try {
			int outPacketSize = 16 + filepathBytes.length;
			out.writeInt(outPacketSize);
			out.writeInt(Master.OPEN_FILE_CMD);
			out.writeInt(filepathBytes.length);
			out.write(filepathBytes);
			out.flush();

			int inPacketSize = in.readInt();
			int retValue = in.readInt();

			if (retValue == FSReturnVals.Success.getValue()) {
				String filename = readString(in, in.readInt());
				ofh.filename = filename;
			}

			return FSReturnVals.valueOf(retValue);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return FSReturnVals.Fail;
	}

	/**
	 * Closes the specified file handle Returns BadHandle if ofh is invalid
	 *
	 * Example usage: CloseFile(FH1)
	 */
	public FSReturnVals CloseFile(FileHandle ofh) {
		// Return the FilePath as a file handle. Vacuous.
		byte[] filenameBytes = ofh.filename.getBytes();

		try {
			int outPacketSize = 16 + filenameBytes.length;
			out.writeInt(outPacketSize);
			out.writeInt(Master.CLOSE_FILE_CMD);
			out.writeInt(filenameBytes.length);
			out.write(filenameBytes);
			out.flush();

			int inPacketSize = in.readInt();
			int retValue = in.readInt();
			return FSReturnVals.valueOf(retValue);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return FSReturnVals.BadHandle;
	}

	private String readString(DataInputStream in, int size) throws IOException {
		byte[] bytes = new byte[size];
		int bytesRead = 0;
		while (bytesRead < size) {
			bytesRead += in.read(bytes, bytesRead, size - bytesRead);
		}
		return new String(bytes);
	}

}
