package com.client;

public class RID {
	private String chunkHandle;
	private int slotNumber; //this is the offset of records from the end, not the number of byte
				//an offset of 1 means it is the second to last record and i must go from the end of the 
				//chunk handle the chunklength - offset * 4
	private int recordLength;
	public RID() {
		slotNumber = -1;
	}
	
	public RID(String chunkHandle, int offset) {
		this.chunkHandle = chunkHandle;
		this.slotNumber = offset;
	}
	
	public void setChunkHandle(String chunkHandle) {
		this.chunkHandle = chunkHandle;
	}
	
	public String getChunkHandle() {
		return this.chunkHandle;
	}
	
	public void setSlotNumber(int slotNumber) {
		this.slotNumber = slotNumber;
	}
	
	public int getSlotNumber() {
		return this.slotNumber;
	}
	
	public void setRecordLength(int length) {
		this.recordLength = length;
	}
	
	public int getRecordLength() {
		return this.recordLength;
	}
	
	public boolean isEmpty() {
		if(slotNumber == -1 || chunkHandle == null)
			return true;
		return false;
	}
	
	public boolean checkValid() {
		return true;
	}
}
