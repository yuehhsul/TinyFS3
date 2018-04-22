package com.client;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.chunkserver.ChunkServer;
import com.client.ClientFS.FSReturnVals;

public class ClientRec {
	
	ChunkServer cs;
	ClientFS cfs;

	public ClientRec() {
		cs = new ChunkServer();
	}
	
	public void init(ClientFS clientFS) {
		cfs = clientFS;
	}
	/**
	 * Appends a record to the open file as specified by ofh Returns BadHandle
	 * if ofh is invalid Returns BadRecID if the specified RID is not null
	 * Returns RecordTooLong if the size of payload exceeds chunksize RID is
	 * null if AppendRecord fails
	 *
	 * Example usage: AppendRecord(FH1, obama, RecID1)
	 */
	public FSReturnVals AppendRecord(FileHandle ofh, byte[] payload, RID RecordID) {
		if(ofh == null) {
			return FSReturnVals.BadHandle;
		}
		if(ofh.checkValid()==false) {
			return FSReturnVals.BadHandle;
		}
		if (RecordID == null) {	//TODO: ask if there is a mistake in the comment
			return FSReturnVals.BadRecID;
		}
//		if(payload.length > ChunkServer.ChunkSize) {
//			return FSReturnVals.RecordTooLong;
//		}
		ArrayList<String> chunkList = ofh.getChunkList();
		String lastChunk = chunkList.get(chunkList.size()-1);
		if(cs.AppendRecord(lastChunk, payload, RecordID)==FSReturnVals.Fail) {
			RecordID = null;
			return FSReturnVals.Fail;
		}
		else if(cs.AppendRecord(lastChunk, payload, RecordID)==FSReturnVals.RecordTooLong) {
			System.out.println("creating new chunk");
			System.out.println("ofhgetdir = "+ofh.getDir());
			ofh = cfs.createNewChunk(ofh.getDir(), ofh.getName());
			return AppendRecord(ofh, payload, RecordID);
//			return FSReturnVals.RecordTooLong;
		}
		return FSReturnVals.Success;
	}

	/**
	 * Deletes the specified record by RecordID from the open file specified by
	 * ofh Returns BadHandle if ofh is invalid Returns BadRecID if the specified
	 * RID is not valid Returns RecDoesNotExist if the record specified by
	 * RecordID does not exist.
	 *
	 * Example usage: DeleteRecord(FH1, RecID1)
	 */
	public FSReturnVals DeleteRecord(FileHandle ofh, RID RecordID) {
		if(ofh == null) {
			return FSReturnVals.BadHandle;
		}
		if(ofh.checkValid()==false) {
			return FSReturnVals.BadHandle;
		}
		if(ofh.isEmpty()) {
			return FSReturnVals.RecDoesNotExist;
		}
		if (RecordID == null) {	//TODO: ask if there is a mistake in the comment
			return FSReturnVals.BadRecID;
		}
		if(!RecordID.checkValid()) {
			return FSReturnVals.BadRecID;
		}
		String chunkHandle = RecordID.getChunkHandle();
		
		return cs.DeleteRecord(chunkHandle, RecordID);
	}

	/**
	 * Reads the first record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadFirstRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadFirstRecord(FileHandle ofh, TinyRec rec){
		if(ofh == null) {
			return FSReturnVals.BadHandle;
		}
		if(ofh.checkValid()==false) {
			return FSReturnVals.BadHandle;
		}
		if(ofh.isEmpty()) {
			return FSReturnVals.RecDoesNotExist;
		}
		ArrayList<String> chunkList = ofh.getChunkList();
		String firstChunk = chunkList.get(0);
		
		return cs.ReadFirstRecord(firstChunk, rec);
	}

	/**
	 * Reads the last record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadLastRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadLastRecord(FileHandle ofh, TinyRec rec){
		if(ofh == null) {
			return FSReturnVals.BadHandle;
		}
		if(ofh.checkValid()==false) {
			return FSReturnVals.BadHandle;
		}
		if(ofh.isEmpty()) {
			return FSReturnVals.RecDoesNotExist;
		}
		ArrayList<String> chunkList = ofh.getChunkList();
		String lastChunk = chunkList.get(chunkList.size()-1);
		
		return cs.ReadLastRecord(lastChunk, rec);
	}

	/**
	 * Reads the next record after the specified pivot of the file specified by
	 * ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadFirstRecord(FH1, tinyRec1) 2. ReadNextRecord(FH1,
	 * rec1, tinyRec2) 3. ReadNextRecord(FH1, rec2, tinyRec3)
	 */
	public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, TinyRec rec){
		if(ofh == null) {
			return FSReturnVals.BadHandle;
		}
		if(ofh.checkValid()==false) {
			return FSReturnVals.BadHandle;
		}
		if(ofh.isEmpty()) {
			return FSReturnVals.RecDoesNotExist;
		}
		if(pivot==null) {
			rec.setRID(null);
			return FSReturnVals.RecDoesNotExist;
		}
		if(!pivot.checkValid()) {
			rec.setRID(null);
			return FSReturnVals.RecDoesNotExist;
		}
		String chunkHandle = pivot.getChunkHandle();
		int slotNum = pivot.getSlotNumber();
		boolean lastChunk = false;
		if(ofh.isLastChunk(chunkHandle)) {
			lastChunk = true;
		}
		String nextChunkHandle = ofh.getNextChunk(chunkHandle);
		
		return cs.ReadNextRecord(chunkHandle, nextChunkHandle, rec, lastChunk, slotNum);
	}

	/**
	 * Reads the previous record after the specified pivot of the file specified
	 * by ofh into payload Returns BadHandle if ofh is invalid Returns
	 * RecDoesNotExist if the file is empty or pivot is invalid
	 *
	 * Example usage: 1. ReadLastRecord(FH1, tinyRec1) 2. ReadPrevRecord(FH1,
	 * recn-1, tinyRec2) 3. ReadPrevRecord(FH1, recn-2, tinyRec3)
	 */
	public FSReturnVals ReadPrevRecord(FileHandle ofh, RID pivot, TinyRec rec){
		if(ofh == null) {
			return FSReturnVals.BadHandle;
		}
		if(ofh.checkValid()==false) {
			return FSReturnVals.BadHandle;
		}
		if(ofh.isEmpty()) {
			return FSReturnVals.RecDoesNotExist;
		}
		if(pivot==null) {
			return FSReturnVals.RecDoesNotExist;
		}
		if(!pivot.checkValid()) {
			return FSReturnVals.RecDoesNotExist;
		}
		String chunkHandle = pivot.getChunkHandle();
		int slotNum = pivot.getSlotNumber();
		
		boolean firstChunk = false;
		if(ofh.isFirstChunk(chunkHandle)) {
			firstChunk = true;
		}
		String prevChunkHandle = ofh.getNextChunk(chunkHandle);
		
		return cs.ReadPrevRecord(chunkHandle, prevChunkHandle, rec, firstChunk, slotNum);
	}

}
