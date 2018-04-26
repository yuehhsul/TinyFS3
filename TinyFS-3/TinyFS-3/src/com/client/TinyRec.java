package com.client;

public class TinyRec {
	private byte[] payload = null;
	private RID ID = null;
	private boolean hasSub = false;
	
	public byte[] getPayload() {
		return payload;
	}
	public void setPayload(byte[] p) {
		payload = p;
	}
	
	public RID getRID() {
		return ID;
	}
	public void setRID(RID inputID) {
		ID = inputID;
	}
	
	public void setHasSub() {
		hasSub = true;
	}
	
	public boolean hasSub() {
		return hasSub;
	}
}
