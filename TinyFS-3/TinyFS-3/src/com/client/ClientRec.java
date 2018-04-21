package com.client;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import com.chunkserver.ChunkServer;
import com.client.ClientFS.FSReturnVals;

public class ClientRec {
	
	ChunkServer cs;

	ClientRec() {
		cs = new ChunkServer();
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
		if(!RecordID.checkValid()) {
			return FSReturnVals.BadRecID;
		}
		if(payload.length > ChunkServer.ChunkSize)
			return FSReturnVals.RecordTooLong;
		ArrayList<String> chunkList = ofh.getChunkList();
		String lastChunk = chunkList.get(chunkList.size() -1);
//		RecordID.setChunkHandle(lastChunk);	
		if(cs.AppendRecord(lastChunk, payload, RecordID) == FSReturnVals.Fail) {
			RecordID = null;
			return FSReturnVals.Fail;
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
		
		return null;
	}

	/**
	 * Reads the last record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadLastRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadLastRecord(FileHandle ofh, TinyRec rec){
		return null;
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
		return null;
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
		return null;
	}

}
