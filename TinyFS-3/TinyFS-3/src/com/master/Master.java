package com.master;

import com.client.FileHandle;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Map;

import com.client.ClientFS.FSReturnVals;

public class Master {
	private static Map<String, ArrayList<String>>fileNSMap;
	
	public enum FSReturnVals {
		DirExists, // Returned by CreateDir when directory exists
		DirNotEmpty, //Returned when a non-empty directory is deleted
		SrcDirNotExistent, // Returned when source directory does not exist
		DestDirExists, // Returned when a destination directory exists
		FileExists, // Returned when a file exists
		FileDoesNotExist, // Returns when a file does not exist
		BadHandle, // Returned when the handle for an open file is not valid
		RecordTooLong, // Returned when a record size is larger than chunk size
		BadRecID, // The specified RID is not valid, used by DeleteRecord
		RecDoesNotExist, // The specified record does not exist, used by DeleteRecord
		NotImplemented, // Specific to CSCI 485 and its unit tests
		Success, //Returned when a method succeeds
		Fail //Returned when a method fails
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
		//Check src directory exists
		if(!fileNSMap.containsKey(src)) {
			return FSReturnVals.SrcDirNotExistent;
		}
		
		if(fileNSMap.containsKey(src+dirname)) {
			System.out.println("CreateDir: directory already exists");
			return FSReturnVals.DestDirExists;
		}
		
		//Store in parent dir as full path + dirname
		ArrayList<String> srcDir = fileNSMap.get(src);
		srcDir.add(src+dirname);
		fileNSMap.put(src+dirname, null);
		System.out.println("CreateDir: directory creation successful");
	    return FSReturnVals.Success;
	}

	/**
	 * Deletes the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: DeleteDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals DeleteDir(String src, String dirname) {
		//Check src directory exists
		if(!fileNSMap.containsKey(src)) {
			return FSReturnVals.SrcDirNotExistent;
		}
		
		//Check dirname directory exists, if yes, delete
		if(fileNSMap.containsKey(src+dirname)) {
			ArrayList<String> parentContent = fileNSMap.get(src);
			parentContent.remove(src);
			fileNSMap.remove(src+dirname);
			System.out.println("directory deletion successful");
		    return FSReturnVals.DestDirExists;
		}
		
		System.out.println("dirname directory does not exist");
		return FSReturnVals.Fail;
		
	}
	
	public static boolean deleteDirectory(File dir) {
	    if(dir.exists()){
	        File[] files = dir.listFiles();
	        if(null!=files){
	            for(int i=0; i<files.length; i++) {
	                if(files[i].isDirectory()) {
	                    deleteDirectory(files[i]);
	                }
	                else {
	                    files[i].delete();
	                }
	            }
	        }
	    }
	    return(dir.delete());
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
		//Check src directory exists
		if(!fileNSMap.containsKey(src)) {
			return FSReturnVals.SrcDirNotExistent;
		}
		//Check if newDir already exists
		if(fileNSMap.containsKey(NewName)) {
			return FSReturnVals.DestDirExists;
		}
		
		//Find parent directory and modify content
		//-2 because src is in ../../../ format, so -2 removes the last "/" and lastIndexOf would
		//return the second to last
		String parentDir = src.substring(0, src.substring(0, src.length()-2).lastIndexOf("/"));
		if(!fileNSMap.containsKey(parentDir)) {
			System.out.println("Rename: parent directory does not exist");
			return FSReturnVals.Fail;
		}
		ArrayList<String> parentContent = fileNSMap.get(parentDir);
		parentContent.remove(src);
		parentContent.add(NewName);
		//Rename by removing and adding back new name
		ArrayList<String> srcDirContent = fileNSMap.remove(src);
		fileNSMap.put(NewName, srcDirContent);
		return FSReturnVals.Success;
	}

	/**
	 * Lists the content of the target directory Returns SrcDirNotExistent if
	 * the target directory does not exist Returns null if the target directory
	 * is empty
	 *
	 * Example usage: ListDir("/Shahram/CSCI485")
	 */
	public String[] ListDir(String tgt) {
		File tgtDir = new File(tgt);
		if (!tgtDir.isDirectory()) {
			return null;
		}
		
		String[] tgtList = tgtDir.list();
		
		//return null if directory is empty
		if(tgtList.length==0) return null;
		
		return tgtList;
	}

	/**
	 * Creates the specified filename in the target directory Returns
	 * SrcDirNotExistent if the target directory does not exist Returns
	 * FileExists if the specified filename exists in the specified directory
	 *
	 * Example usage: Createfile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals CreateFile(String tgtdir, String filename) {
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
	
	public static void main(String args[])
	{
		//Create the hashmap (populate it)
		//Accept client connections
	}


}
