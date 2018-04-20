package com.client;

public class RID {
	private String chunkHandle;
	private int slotNumber; //this is the offset of records from the end, not the number of byte
				//an offset of 1 means it is the second to last record and i must go from the end of the 
				//chunk handle the chunklength - offset * 4
	public RID() {
		int offset = -1;
	}
	
	public RID(String chunkHandle, int offset) {
		this.chunkHandle = chunkHandle;
		this.slotNumber = offset;
	}
	
	public void setChunkHandle(String chunkHandle) {
		this.chunkHandle = chunkHandle;
	}
	
	public void setOffset(int offset) {
		this.slotNumber = offset;
	}
	
	public int getOffset() {
		return this.slotNumber;
	}
	
	public String getChunkHandle() {
		return this.chunkHandle;
	}
	
	public boolean isEmpty() {
		if(slotNumber == -1 || chunkHandle == null)
			return true;
		return false;
	}
}
