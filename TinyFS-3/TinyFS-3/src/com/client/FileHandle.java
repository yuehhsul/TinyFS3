package com.client;

import java.util.ArrayList;
import java.util.Map;

public class FileHandle {
	private Map<String, String> chunkAddrMap; //map of chunkhandle to the chunkServer
	private ArrayList<String> chunkList; //list of the chunkHandles
	
	public FileHandle() {}
	
	public FileHandle(Map<String, String> map, ArrayList<String> list) {
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
	
	public boolean checkValid() {
		return true;
	}
}
