package com.client;

public class RID {
	String chunkHandle;
	int offset; //this is the offset of records from the end, not the number of byte
				//an offset of 1 means it is the second to last record and i must go from the end of the 
				//chunk handle the chunklength - offset * 4
	public RID() {
	
	}
	
	public RID(String chunkHandle, int offset) {
		this.chunkHandle = chunkHandle;
		this.offset = offset;
	}
	
	public void setChunkHandle() {
		
	}
	
	public void setOffset() {
		
	}
}
