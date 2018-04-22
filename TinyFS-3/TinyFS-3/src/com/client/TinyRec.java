package com.client;

public class TinyRec {
	private static byte[] payload = null;
	private static RID ID = null;
	
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
}
