package com.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class FileHandle {
	private static Map<String, String> chunkAddrMap; //map of chunkhandle to the chunkServer
	private static ArrayList<String> chunkList; //list of the chunkHandles
	private static String fileName;
	private static String dir;
	
	public FileHandle() {
		chunkAddrMap = new HashMap<String, String>();
		chunkList = new ArrayList<String>();
	}
	
	public FileHandle(Map<String, String> map, ArrayList<String> list, String parentDir, String filename) {
		if(map==null || list==null) {
			System.out.println("FileHandle constructor cannot have null map or list");
			return;
		}
		chunkAddrMap = map;
		chunkList = list;
		fileName = filename;
		dir = parentDir;
	}
	
	public String getDir() {
		return dir;
	}
	
	public String getName() {
		return fileName;
	}
	
	public ArrayList<String> getChunkList() {
		return chunkList;
	}
	
	public Map<String, String> getChunkAddrMap() {
		return chunkAddrMap;
	}
	
	public void addChunk(String chunk) {
		chunkList.add(chunk);
		chunkAddrMap.put(chunk, "");
	}
	
	public String getNextChunk(String chunk) {
		for(int i=0;i<chunkList.size();i++) {
			if(chunkList.get(i).equals(chunk)) {
				if(i+1<=chunkList.size()-1) {
					return chunkList.get(i+1);
				}
			}
		}
		return null;
	}
	
	public String getPrevChunk(String chunk) {
		for(int i=0;i<chunkList.size();i++) {
			if(chunkList.get(i).equals(chunk)) {
				if(i-1>=0) {
					return chunkList.get(i-1);
				}
			}
		}
		return null;
	}
	
	public boolean isLastChunk(String chunkHandle) {
		if(chunkHandle.equals(chunkList.get(chunkList.size()-1))) return true;
		return false;
	}
	
	public boolean isFirstChunk(String chunkHandle) {
		if(chunkHandle.equals(chunkList.get(0))) return true;
		return false;
	}
	
	public boolean isEmpty() {
//		System.out.println("chunkList size is "+chunkList.size());
		if(chunkList==null || chunkAddrMap==null) return true;
//		System.out.println("both nonnull, chunkList size is "+chunkList.size());
		return chunkList.size()==0;
	}
	
	public boolean checkValid() {
//		if(isEmpty()) return false;
		return true;
	}
	
	public void clear() {
		chunkAddrMap.clear();
		chunkList = new ArrayList<String>();
	}
}
