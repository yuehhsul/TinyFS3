package com.master;

import com.client.FileHandle;
import com.client.RID;
import com.client.TinyRec;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.chunkserver.ChunkServer;
import com.client.ClientFS.FSReturnVals;

public class Master {
	private static Map<String, ArrayList<String>>fileNSMap;
	private static Map<String, ArrayList<String>>fileHandleMap;
	private static Map<String, String> chunkAddrMap;
	ChunkServer cs;
	
	//Defined ints for log records
	public static final int createDirCMD = 1001;
	public static final int deleteDirCMD = 1002;
	public static final int renameDirCMD = 1003;
	public static final int createFileCMD = 1004;
	public static final int deleteFileCMD = 1005;
	public static final int createChunkCMD = 1006;
	
	//Boolean when recovering
	private static boolean isRecovering = false;
	private FileHandle logfh;
	
	//Filepath to log dir
	private static String logFilepath = "log/";
	
	private static Master master;
	
	private int chunkServerID;
	private final int normType = 22;
	
	public Master() {
		chunkServerID = 0;
		fileNSMap = new HashMap<String, ArrayList<String>>();
		fileNSMap.put("/", new ArrayList<String>());
		fileHandleMap = new HashMap<String, ArrayList<String>>();
		chunkAddrMap = new HashMap<String, String>();
//		cs = new ChunkServer(chunkServerID);
		cs = new ChunkServer();
		this.logRecover();
	}
	
	private byte[] prependType(int type, byte[] payload) {
		ByteBuffer bb = ByteBuffer.allocate(4+payload.length);
		bb.putInt(type);
		bb.put(payload);
		return bb.array();
	}

	/**
	 * Creates the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: CreateDir("/", "Shahram"), CreateDir("/Shahram/",
	 * "CSCI485"), CreateDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals CreateDir(String srcDirectory, String dirname) {
		String fullPath = srcDirectory+dirname;
		String src = srcDirectory;
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
		
		if(!isRecovering) {	//Only write log records when not recovering namespaces
			cs.writeToLog(true);
			for(int i=0;i<3;i++) {
				System.out.println("Reached log write in createDIR--------");
				RID RecordID = new RID();
				switch(i) {
					case 0:	//append commanddtype
						ByteBuffer bb = ByteBuffer.allocate(4);
						bb.putInt(createDirCMD);
//						System.out.println("fh name is "+fh.getName());
						cs.AppendRecord(logfh, prependType(normType, bb.array()), RecordID);
						break;
					case 1:	//append argOne
						byte[] srcBA = srcDirectory.getBytes();
						cs.AppendRecord(logfh, prependType(normType, srcBA), RecordID);
						break;
					case 2:
						System.out.println("Reached:"+i);
						byte[] nameBA = dirname.getBytes();
						cs.AppendRecord(logfh, prependType(normType, nameBA), RecordID);
						break;
					default:
						break;
				}
			}
			cs.writeToLog(false);
		}
		
	    return FSReturnVals.Success;
	}

	/**
	 * Deletes the specified dirname in the src directory Returns
	 * SrcDirNotExistent if the src directory does not exist Returns
	 * DestDirExists if the specified dirname exists
	 *
	 * Example usage: DeleteDir("/Shahram/CSCI485/", "Lecture1")
	 */
	public FSReturnVals DeleteDir(String srcDir, String dirname) {
		String fullPath = srcDir+dirname;
		String src = srcDir;
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
			
			if(!isRecovering) {	//Only write log records when not recovering namespaces
				cs.writeToLog(true);
				for(int i=0;i<3;i++) {
					RID RecordID = new RID();
					switch(i) {
						case 0:	//append commanddtype
							ByteBuffer bb = ByteBuffer.allocate(4);
							bb.putInt(deleteDirCMD);
							cs.AppendRecord(logfh, prependType(normType, bb.array()), RecordID);
							break;
						case 1:	//append argOne
							byte[] srcBA = srcDir.getBytes();
							cs.AppendRecord(logfh, prependType(normType, srcBA), RecordID);
							break;
						case 2:
							byte[] nameBA = dirname.getBytes();
							cs.AppendRecord(logfh, prependType(normType, nameBA), RecordID);
							break;
						default:
							break;
					}
				}
				cs.writeToLog(false);
			}
			
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
		
		if(!isRecovering) {	//Only write log records when not recovering namespaces
			cs.writeToLog(true);
			for(int i=0;i<3;i++) {
				RID RecordID = new RID();
				switch(i) {
					case 0:	//append commanddtype
						ByteBuffer bb = ByteBuffer.allocate(4);
						bb.putInt(renameDirCMD);
						cs.AppendRecord(logfh, prependType(normType, bb.array()), RecordID);
						break;
					case 1:	//append argOne
						byte[] srcBA = src.getBytes();
						cs.AppendRecord(logfh, prependType(normType, srcBA), RecordID);
						break;
					case 2:
						byte[] nameBA = NewName.getBytes();
						cs.AppendRecord(logfh, prependType(normType, nameBA), RecordID);
						break;
					default:
						break;
				}
			}
			cs.writeToLog(false);
		}
		
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
	public FSReturnVals CreateFile(String dir, String filename) {
		String tgtdir = dir;
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
		
		System.out.println("fileFullname is "+fileFullName);
		
		//Call createChunk on cs
		String chunkHandle = cs.createChunk();
		
		//Add file to ns
		dirContent.add(fileFullName);
		fileNSMap.put(tgtdir, dirContent);
		//Add file to fileHandleMap
		ArrayList<String> chunkList = new ArrayList<String>();
		chunkList.add(chunkHandle);
		fileHandleMap.put(fileFullName, chunkList);
		System.out.println("putting "+chunkHandle+" in "+fileFullName);
		chunkAddrMap.put(chunkHandle, "csci485");
		
		if(!isRecovering) {	//Only write log records when not recovering namespaces
			cs.writeToLog(true);
			for(int i=0;i<3;i++) {
				RID RecordID = new RID();
//				System.out.println("fh name is "+fh.getName());
				switch(i) {
					case 0:	//append commanddtype
						ByteBuffer bb = ByteBuffer.allocate(4);
						bb.putInt(createFileCMD);
						cs.AppendRecord(logfh, prependType(normType, bb.array()), RecordID);
						break;
					case 1:	//append argOne
						byte[] srcBA = dir.getBytes();
						cs.AppendRecord(logfh, prependType(normType, srcBA), RecordID);
						break;
					case 2:
						byte[] nameBA = filename.getBytes();
						cs.AppendRecord(logfh, prependType(normType, nameBA), RecordID);
						break;
					default:
						break;
				}
			}
			cs.writeToLog(false);
		}
		System.out.println("Create file successful");
		return FSReturnVals.Success;
	}
	
	public FileHandle createNewChunk(String tgtdir, String filename) {
//		System.out.println("cncfh name is "+fh.getName());
		//Check tgtdir directory exists
		if(!fileNSMap.containsKey(tgtdir)) {
			System.out.println("CreateChunk: tgtdir does not exist");
			return null;
		}
		
		ArrayList<String> dirContent = fileNSMap.get(tgtdir);
		if(!dirContent.contains(filename)) {
			System.out.println("CreateChunk: no such file");
			return null;
		}
		
		//Call createChunk on cs
		String chunkHandle = cs.createChunk();
		
		ArrayList<String> chunkList = fileHandleMap.get(filename);
		chunkList.add(chunkHandle);
		fileHandleMap.put(filename, chunkList);
		chunkAddrMap.put(chunkHandle, "csci485");
		Map<String, String> map = new HashMap<String, String>();
//		System.out.println("cncfh name is "+fh.getName());
		for(int i=0;i<chunkList.size();i++) {
			String chunkhandle = chunkList.get(i);
			String chunkAddr = chunkAddrMap.get(chunkhandle);
			map.put(chunkhandle, chunkAddr);
		}
		String dir = filename;
		int occurrences = filename.length() - filename.replace("/", "").length();
		if(occurrences>1) {
			dir = filename.substring(0, filename.lastIndexOf("/"));
		}
		FileHandle returnFh = new FileHandle(map, chunkList, dir, filename);
		
//		if(!isRecovering) {	//Only write log records when not recovering namespaces
//			cs.writeToLog(true);
//			for(int i=0;i<3;i++) {
//				RID RecordID = new RID();
////				System.out.println("fh name is "+fh.getName());
//				switch(i) {
//					case 0:	//append commanddtype
//						ByteBuffer bb = ByteBuffer.allocate(4);
//						bb.putInt(createChunkCMD);
//						cs.AppendRecord(logfh, bb.array(), RecordID);
//						break;
//					case 1:	//append argOne
//						byte[] srcBA = tgtdir.getBytes();
//						cs.AppendRecord(logfh, srcBA, RecordID);
//						break;
//					case 2:
//						byte[] nameBA = filename.getBytes();
//						cs.AppendRecord(logfh, nameBA, RecordID);
//						break;
//					default:
//						break;
//				}
//			}
//			cs.writeToLog(false);
//		}
		
		return returnFh;
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
		
		if(!isRecovering) {	//Only write log records when not recovering namespaces
			cs.writeToLog(true);
			for(int i=0;i<3;i++) {
				RID RecordID = new RID();
				switch(i) {
					case 0:	//append commanddtype
						ByteBuffer bb = ByteBuffer.allocate(4);
						bb.putInt(deleteFileCMD);
						cs.AppendRecord(logfh, prependType(normType, bb.array()), RecordID);
						break;
					case 1:	//append argOne
						byte[] srcBA = tgtdir.getBytes();
						cs.AppendRecord(logfh, prependType(normType, srcBA), RecordID);
						break;
					case 2:
						byte[] nameBA = filename.getBytes();
						cs.AppendRecord(logfh, prependType(normType, nameBA), RecordID);
						break;
					default:
						break;
				}
			}
			cs.writeToLog(false);
		}
		
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
		Map<String, String> fileChunkAddrMap = new HashMap<String, String>();
		for(int i=0;i<chunkList.size();i++) {
			String chunkHandle = chunkList.get(i);
			String chunkAddr = chunkAddrMap.get(chunkHandle);
			fileChunkAddrMap.put(chunkHandle, chunkAddr);
		}
		String dir = FilePath;
		int occurrences = FilePath.length() - FilePath.replace("/", "").length();
		if(occurrences>1) {
			dir = FilePath.substring(0, FilePath.lastIndexOf("/"));
		}
		FileHandle openfh = new FileHandle(fileChunkAddrMap, chunkList, dir, FilePath);
		ofh = openfh;
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
	
	private void logRecover() {
		isRecovering = true;
		File dir = new File(logFilepath);
		File[] fs = dir.listFiles();

		System.out.println("logRecovering-----------------"+fs.length);

		ArrayList<String> logList = new ArrayList<String>();
		Map<String, String> logMap = new HashMap<String, String>();
		if(fs.length == 0){
			cs.writeToLog(true);
			this.CreateDir("/", "log");
			this.CreateFile("/log/", "logRecord");
			cs.writeToLog(false);
			dir = new File(logFilepath);
			System.out.println("------dir is =---------"+dir);
			fs = dir.listFiles();
			System.out.println("------fs.length is =---------"+fs.length);
			for (int j=0; j < fs.length; j++) {
				System.out.println("-----------------here------------");
				logList.add(fs[j].getName());
				logMap.put(fs[j].getName(), "log");
			}
			Collections.sort(logList);
			logfh = new FileHandle(logMap, logList, "/log", "/log/logRecord");
			System.out.println("No log records to recover");
			
			isRecovering = false;
			return;
		}else{
			for (int j=0; j < fs.length; j++) {
				logList.add(fs[j].getName());
				logMap.put(fs[j].getName(), "log");
			}
			Collections.sort(logList);
			logfh = new FileHandle(logMap, logList, "/log", "/log/logRecord");
			TinyRec r1 = new TinyRec();
			cs.readFromLog(true);
			System.out.println("Reading first");
			FSReturnVals retRR = cs.ReadFirstRecord(logfh, r1);
			cs.readFromLog(false);
			if(retRR != FSReturnVals.Success ){
				System.out.println("logRecover ReadFirstRecord Fail: "+retRR);
				isRecovering = false;
	    		return;
			}

			int typeCounter = 1; //0 is cmd, 1 is first arg, 2 is second arg
			int cmdType = -1;
			if(r1.getRID()!=null) {
				byte[] ba = r1.getPayload();
				ByteBuffer cmdbb = ByteBuffer.wrap(ba);
				cmdType = cmdbb.getInt();
			}
			String argOne = null;
			String argTwo = null;
			while (r1.getRID() != null){
				TinyRec r2 = new TinyRec();
				cs.readFromLog(true);
				System.out.println("Reading next:"+typeCounter);
				cs.ReadNextRecord(logfh, r1.getRID(), r2);
				cs.readFromLog(false);
				if(r2.getRID() != null){
					//Get payload
					byte[] instrBA = r2.getPayload();
					System.out.println("typecounter is ="+typeCounter);
					switch(typeCounter) {
						case 0: //Get cmd
							ByteBuffer bb = ByteBuffer.wrap(instrBA);
							cmdType = bb.getInt();
							break;
						case 1: //Get first arg
							argOne = (new String(instrBA)).toString();
							break;
						case 2: //Get second arg and execute if no error
							argTwo = (new String(instrBA)).toString();
							System.out.println(cmdType+" "+argOne+" "+argTwo);
							if(cmdType>0 && argOne!=null && argTwo!=null) {
								switch(cmdType) {
									case createDirCMD:
										System.out.println("calling: CreateDir("+argOne+", "+argTwo+")");
										this.CreateDir(argOne, argTwo);
										break;
									case deleteDirCMD:
										System.out.println("calling: deleteDir("+argOne+", "+argTwo+")");
										this.DeleteDir(argOne, argTwo);
										break;
									case renameDirCMD:
										System.out.println("calling: renameDir("+argOne+", "+argTwo+")");
										this.RenameDir(argOne, argTwo);
										break;
									case createFileCMD:
										System.out.println("calling: createFile("+argOne+", "+argTwo+")");
										this.CreateFile(argOne, argTwo);
										break;
									case deleteFileCMD:
										System.out.println("calling: deleteFile("+argOne+", "+argTwo+")");
										this.DeleteFile(argOne, argTwo);
										break;
									case createChunkCMD:
										System.out.println("calling: createNewChunk("+argOne+", "+argTwo+")");
										this.createNewChunk(argOne, argTwo);
										break;
									default:
										System.out.println("default reached");
										break;
								}
							}
							break;
						default: 
							break;
					}
					r1 = r2;
					typeCounter += 1;
					typeCounter %= 3;
				} else {
					r1.setRID(null);
				}
			}
		}
		isRecovering = false;
	}
	
	public static void main(String args[])
	{
		//Create the hashmap (populate it)
		//Accept client connections
		master = new Master();
	}


}
