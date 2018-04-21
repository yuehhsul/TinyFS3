package com.client;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class FileHandle {
	private Map<String, String> chunkAddrMap; //map of chunkhandle to the chunkServer
	private ArrayList<String> chunkList; //list of the chunkHandles
	
	public FileHandle() {
		chunkAddrMap = new HashMap<String, String>();
		chunkList = new ArrayList<String>();
	}
	
	public FileHandle(Map<String, String> map, ArrayList<String> list) {
		if(map==null || list==null) {
			System.out.println("FileHandle constructor cannot have null map or list");
			return;
		}
		chunkAddrMap = map;
		chunkList = list;
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
	
	public boolean isEmpty() {
		if(chunkList==null || chunkAddrMap==null) return true;
		return chunkList.size()==0;
	}
	
	public boolean checkValid() {
		if(isEmpty()) return false;
		return true;
	}
}
