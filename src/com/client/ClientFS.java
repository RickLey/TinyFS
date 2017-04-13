package com.client;

import com.master.Master;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
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
	private ObjectOutputStream outStream;
	private ObjectInputStream inStream;

	public ClientFS() {
		try {
			socket = new Socket(Master.HOST, Master.PORT);
			outStream = new ObjectOutputStream(socket.getOutputStream());
			inStream = new ObjectInputStream(socket.getInputStream());
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
		return null;
	}

	/**
	 * Deletes the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: DeleteDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals DeleteDir(String src, String dirname) {
		return null;
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
		return null;
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
		return null;
	}

	/**
	 * Deletes the specified filename from the tgtdir Returns SrcDirNotExistent
	 * if the target directory does not exist Returns FileDoesNotExist if the
	 * specified filename is not-existent
	 *
	 * Example usage: DeleteFile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals DeleteFile(String tgtdir, String filename) {
		return null;
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
		return null;
	}

	/**
	 * Closes the specified file handle Returns BadHandle if ofh is invalid
	 *
	 * Example usage: CloseFile(FH1)
	 */
	public FSReturnVals CloseFile(FileHandle ofh) {
		return null;
	}

}
