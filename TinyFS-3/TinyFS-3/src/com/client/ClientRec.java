package com.client;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import com.chunkserver.ChunkServer;
import com.client.ClientFS.FSReturnVals;

public class ClientRec {
	
	ChunkServer cs;
	ClientFS cfs;

	public ClientRec() {
		cs = new ChunkServer(0);
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
		//Case where payload is larger than max chunk size
		int maxChunkSize = 4080;
		if(payload.length>maxChunkSize) {
			int startIndex = 0;
			int endIndex = -1;

			FileHandle tempfh = ofh;
			String metaChunk = ofh.getLastChunk();
			//metarecord
			if(cs.getEmptySpace(ofh.getLastChunk())<0) {
				tempfh = cfs.createNewChunk(ofh.getDir(), ofh.getName());
				metaChunk = ofh.getLastChunk();
			}
			byte[] metaba = "meta".getBytes();
			if(cs.AppendRecord(tempfh, metaba, RecordID) == FSReturnVals.Fail) {
				RecordID = null;
			}
			
			while(true) {
				endIndex = startIndex + maxChunkSize;
				int prevChunkEmptySpace = cs.getEmptySpace(ofh.getLastChunk());
				
				if(endIndex>=payload.length) {
					tempfh = cfs.createNewChunk(ofh.getDir(), ofh.getName());
					System.out.println("Appending last subrecord!------------------------size ="+String.valueOf(payload.length-startIndex));
					byte[] subPayload = Arrays.copyOfRange(payload, startIndex, payload.length);
					FSReturnVals lastappfs = cs.AppendRecord(tempfh, subPayload, RecordID);
					if(lastappfs != FSReturnVals.Success) {
						System.out.println("Appending last subrecord still failed");
						if(lastappfs == FSReturnVals.Fail) {
							RecordID = null;
						}
						return lastappfs;
					}
					return AppendRecord(tempfh, metaba, RecordID);
//					return lastappfs;
				}
//				if(prevChunkEmptySpace<maxChunkSize) {
					tempfh = cfs.createNewChunk(ofh.getDir(), ofh.getName());
//				}
				
				byte[] subPayload = Arrays.copyOfRange(payload, startIndex, endIndex);
				FSReturnVals appfs = cs.AppendRecord(tempfh, subPayload, RecordID);
				if(appfs != FSReturnVals.Success) {
					System.out.println("Appending subrecord still failed");
					if(appfs == FSReturnVals.Fail) {
						RecordID = null;
					}
					return appfs;
				}
				startIndex = endIndex;
			}
		}
		
		FSReturnVals retVal = cs.AppendRecord(ofh, payload, RecordID);
		if(retVal==FSReturnVals.Fail) {
			RecordID = null;
			return FSReturnVals.Fail;
		}
		else if(retVal==FSReturnVals.RecordTooLong) {
			System.out.println("creating new chunk");
			System.out.println("ofhgetdir = "+ofh.getDir());
			FileHandle newfh = cfs.createNewChunk(ofh.getDir(), ofh.getName());
			return AppendRecord(newfh, payload, RecordID);
		}
		return retVal;
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
		return cs.DeleteRecord(ofh, RecordID);
	}

	/**
	 * Reads the first record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadFirstRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadFirstRecord(FileHandle ofh, TinyRec rec){
		return cs.ReadFirstRecord(ofh, rec);
	}

	/**
	 * Reads the last record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadLastRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadLastRecord(FileHandle ofh, TinyRec rec){
		return cs.ReadLastRecord(ofh, rec);
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
		return cs.ReadNextRecord(ofh, pivot, rec);
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
		return cs.ReadPrevRecord(ofh, pivot, rec);
	}

}
