package com.client;

public class TinyRec {
	private static byte[] payload = null;
	private static RID ID = null;
	
	public byte[] getPayload() {
		return payload;
	}
	public void setPayload(byte[] p) {
		System.out.println("reached set payload, p size = "+p.length);
		
		payload = p;
	}
	
	public RID getRID() {
		return ID;
	}
	public void setRID(RID inputID) {
		ID = inputID;
	}
}
