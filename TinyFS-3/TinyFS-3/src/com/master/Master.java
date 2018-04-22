package com.master;

import com.client.FileHandle;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.chunkserver.ChunkServer;
import com.client.ClientFS.FSReturnVals;

public class Master {
	private static Map<String, ArrayList<String>>fileNSMap;
	private static Map<String, ArrayList<String>>fileHandleMap;
	private static Map<String, String> chunkAddrMap;
	public ChunkServer cs;
	
	public Master() {
		fileNSMap = new HashMap<String, ArrayList<String>>();
		fileNSMap.put("/", new ArrayList<String>());
		fileHandleMap = new HashMap<String, ArrayList<String>>();
		chunkAddrMap = new HashMap<String, String>();
		cs = new ChunkServer();
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
		String fullPath = src+dirname;
		if(src.length()>2) {
			src = src.substring(0, src.length()-1);
		}
		System.out.println("Substr src = " + src);
		
		if(fileNSMap==null) {
			System.out.println("File Name Space Map is null");
		}
		
		//Check src directory exists
		if(!fileNSMap.containsKey(src)) {
			return FSReturnVals.SrcDirNotExistent;
		}
		
		if(fileNSMap.containsKey(fullPath)) {
			System.out.println("CreateDir: directory already exists");
			return FSReturnVals.DestDirExists;
		}
		
		//Store in parent dir as full path + dirname
		ArrayList<String> srcDir = fileNSMap.get(src);
		srcDir.add(fullPath);
		fileNSMap.put(src, srcDir);
		fileNSMap.put(fullPath, new ArrayList<String>());
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
		String fullPath = src+dirname;
		if(src.length()>2) {
			src = src.substring(0, src.length()-1);
		}
		
		//Check src directory exists
		if(!fileNSMap.containsKey(src)) {
			return FSReturnVals.SrcDirNotExistent;
		}
		
		//Check dirname directory exists, if yes, delete
		if(fileNSMap.containsKey(fullPath)) {
			
			if(fileNSMap.get(fullPath).size()>0) {
				return FSReturnVals.DirNotEmpty;
			}
			
			ArrayList<String> parentContent = fileNSMap.get(src);
			parentContent.remove(fullPath);
			fileNSMap.put(src, parentContent);
			fileNSMap.remove(fullPath);
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
		String parentDir = src;
		int occurrences = src.length() - src.replace("/", "").length();
		if(occurrences>1) {
			parentDir = src.substring(0, src.lastIndexOf("/"));
		}
		if(!fileNSMap.containsKey(parentDir)) {
			System.out.println("Rename: parent directory does not exist: "+parentDir);
			return FSReturnVals.Fail;
		}
		ArrayList<String> parentContent = fileNSMap.get(parentDir);
		if(parentContent==null) {
			System.out.println("Rename: parent directory empty");
			return FSReturnVals.Fail;
		}
		parentContent.remove(src);
		parentContent.add(NewName);
		fileNSMap.put(parentDir, parentContent);
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
		//Check src directory exists
		if(!fileNSMap.containsKey(tgt)) {
			System.out.println("no tgt in fileNSMap");
			return null;
		}
		
		ArrayList<String> tgtList = fileNSMap.get(tgt);
		if(tgtList==null) {
			System.out.println("tgt dir is empty");
			return null;
		}
		
		ArrayList<String> listedList = new ArrayList<String>();
		for(int i=0;i<tgtList.size();i++) {
			getContent(listedList, tgtList.get(i));
		}
		
//		String[] tgtArray = (String[])tgtList.toArray();
		Object[] objectList = listedList.toArray();
		String[] tgtArray =  Arrays.copyOf(objectList,objectList.length,String[].class);
		
		return tgtArray;
	}
	
	public void getContent(ArrayList<String> list, String dir) {
//		System.out.println("--getContent: "+dir);
		list.add(dir);
		if(!fileNSMap.containsKey(dir)) {
			return;
		}
		ArrayList<String> contentList = fileNSMap.get(dir);
		for(int i=0;i<contentList.size();i++) {
			getContent(list, contentList.get(i));
		}
	}

	/**
	 * Creates the specified filename in the target directory Returns
	 * SrcDirNotExistent if the target directory does not exist Returns
	 * FileExists if the specified filename exists in the specified directory
	 *
	 * Example usage: Createfile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals CreateFile(String tgtdir, String filename) {
		if(tgtdir.length()>2) {
			tgtdir = tgtdir.substring(0, tgtdir.length()-1);
		}
		
		//Check tgtdir directory exists
		if(!fileNSMap.containsKey(tgtdir)) {
			System.out.println("CreateFile: tgtdir does not exist");
			return FSReturnVals.SrcDirNotExistent;
		}
		
		ArrayList<String> dirContent = fileNSMap.get(tgtdir);
		String fileFullName = tgtdir + "/" + filename;
		if(dirContent.contains(fileFullName)) {
			System.out.println("CreateFile: file exists");
			return FSReturnVals.FileExists;
		}
		
		//Call createChunk on cs
		String chunkHandle = cs.createChunk();
		
		//Add file to ns
		dirContent.add(fileFullName);
		fileNSMap.put(tgtdir, dirContent);
		//Add file to fileHandleMap
		ArrayList<String> chunkList = new ArrayList<String>();
		chunkList.add(chunkHandle);
		fileHandleMap.put(fileFullName, chunkList);
		return FSReturnVals.Success;
	}

	/**
	 * Deletes the specified filename from the tgtdir Returns SrcDirNotExistent
	 * if the target directory does not exist Returns FileDoesNotExist if the
	 * specified filename is not-existent
	 *
	 * Example usage: DeleteFile("/Shahram/CSCI485/Lecture1/", "Intro.pptx")
	 */
	public FSReturnVals DeleteFile(String tgtdir, String filename) {
		if(tgtdir.length()>2) {
			tgtdir = tgtdir.substring(0, tgtdir.length()-1);
		}
		
		//Check tgtdir directory exists
		if(!fileNSMap.containsKey(tgtdir)) {
			System.out.println("DeleteFile: tgtdir does not exist");
			return FSReturnVals.SrcDirNotExistent;
		}
		
		ArrayList<String> dirContent = fileNSMap.get(tgtdir);
		String fileFullName = tgtdir + "/" + filename;
		if(!dirContent.contains(fileFullName)) {
			System.out.println("DeleteFile: file does not exist");
			return FSReturnVals.FileDoesNotExist;
		}
		//Remove from ns
		dirContent.remove(fileFullName);
		fileNSMap.put(tgtdir, dirContent);
		//Remove from fileHandleMap
		fileHandleMap.remove(fileFullName);
		return FSReturnVals.Success;
	}

	/**
	 * Opens the file specified by the FilePath and populates the FileHandle
	 * Returns FileDoesNotExist if the specified filename by FilePath is
	 * not-existent
	 *
	 * Example usage: OpenFile("/Shahram/CSCI485/Lecture1/Intro.pptx", FH1)
	 */
	public FSReturnVals OpenFile(String FilePath, FileHandle ofh) {
		if(!fileHandleMap.containsKey(FilePath)) {
			System.out.println("File doesn't exist");
			return FSReturnVals.FileDoesNotExist;
		}
		ArrayList<String> chunkList = fileHandleMap.get(FilePath);
		System.out.println("SIZE is "+chunkList.size());
		Map<String, String> fileChunkAddrMap = new HashMap<String, String>();
		for(int i=0;i<chunkList.size();i++) {
			String chunkHandle = chunkList.get(i);
			String chunkAddr = chunkAddrMap.get(chunkHandle);
			fileChunkAddrMap.put(chunkHandle, chunkAddr);
		}
		FileHandle fh = new FileHandle(fileChunkAddrMap, chunkList);
		ofh = fh;
		return FSReturnVals.Success;
	}

	/**
	 * Closes the specified file handle Returns BadHandle if ofh is invalid
	 *
	 * Example usage: CloseFile(FH1)
	 */
	public FSReturnVals CloseFile(FileHandle ofh) {
//		ofh.clear();
		return FSReturnVals.Success;
	}
	
	public static void main(String args[])
	{
		//Create the hashmap (populate it)
		//Accept client connections
		Master master = new Master();
	}


}
