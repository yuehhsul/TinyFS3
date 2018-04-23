package com.client;

public class RID {
	private String chunkHandle;
	private int slotNumber; //this is the offset of records from the end, not the number of byte
				//an offset of 1 means it is the second to last record and i must go from the end of the 
				//chunk handle the chunklength - offset * 4
	private int recordLength;
	public RID() {
		chunkHandle = null;
		slotNumber = -1;
		recordLength = -1;
	}
	
	public RID(String chunkHandleName, int slot, int length) {
		chunkHandle = chunkHandleName;
		slotNumber = slot;
		recordLength = length;
	}
	
	public void setChunkHandle(String chunkHandleName) {
		chunkHandle = chunkHandleName;
	}
	
	public String getChunkHandle() {
		return chunkHandle;
	}
	
	public void setSlotNumber(int slot) {
		slotNumber = slot;
	}
	
	public int getSlotNumber() {
		return slotNumber;
	}
	
	public void setRecordLength(int length) {
		recordLength = length;
	}
	
	public int getRecordLength() {
		return recordLength;
	}
	
	public boolean isEmpty() {
		if(slotNumber == -1 || chunkHandle == null)
			return true;
		return false;
	}
	
	public boolean checkValid() {
//		if(isEmpty()) return false;
		return true;
	}
}
