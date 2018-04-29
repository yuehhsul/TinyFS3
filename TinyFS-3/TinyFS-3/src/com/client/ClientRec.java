package com.client;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import com.chunkserver.ChunkServer;
import com.client.ClientFS.FSReturnVals;

public class ClientRec {
	
	ArrayList<ChunkServer> chunkServers;
	ClientFS cfs;
	
	//Record types
	private final int metaType = 20;
	private final int subType = 21;
	private final int normType = 22;

	public ClientRec() {
		chunkServers = new ArrayList<ChunkServer>();
		for(int i = 0; i < 4; i++) {
			ChunkServer cs = new ChunkServer();
			chunkServers.add(cs);
		}
	}
	
	public void init(ClientFS clientFS) {
		cfs = clientFS;
	}
	
	private byte[] prependType(int type, byte[] payload) {
		ByteBuffer bb = ByteBuffer.allocate(4+payload.length);
		bb.putInt(type);
		bb.put(payload);
		return bb.array();
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
		int maxChunkSize = 4076; //4076
		if(payload.length>maxChunkSize) {
			int startIndex = 0;
			int endIndex = -1;
			FileHandle tempfh = ofh;
			ArrayList<Integer> subInfoList = new ArrayList<Integer>();

			int prevChunkEmptySpace = chunkServers.get(0).getEmptySpace(ofh.getLastChunk());
			
			while(true) {
				endIndex = startIndex + maxChunkSize;
				
				if(endIndex>=payload.length) {
					tempfh = cfs.createNewChunk(ofh.getDir(), ofh.getName());
					System.out.println("Appending last subrecord!------------------------size ="+String.valueOf(payload.length-startIndex));
					byte[] subPayload = Arrays.copyOfRange(payload, startIndex, payload.length);
					FSReturnVals lastappfs = chunkServers.get(0).AppendRecord(tempfh, prependType(subType, subPayload), RecordID);
					if(lastappfs != FSReturnVals.Success) {
						System.out.println("Appending last subrecord still failed");
						if(lastappfs == FSReturnVals.Fail) {
							RecordID = null;
						}
						return lastappfs;
					}
					//tell replicas to append subrecord
					
					for(int i=1; i < 4; i++) {
						chunkServers.get(i).AppendRecord(tempfh, prependType(subType, subPayload), RecordID);
					}
					
					int intChunkHandle = Integer.parseInt(tempfh.getLastChunk());
					subInfoList.add(intChunkHandle);
					subInfoList.add(0);
					
					ByteBuffer metabb = ByteBuffer.allocate(subInfoList.size()*4);
					for(int i=0;i<subInfoList.size();i++) {
						metabb.putInt(subInfoList.get(i));
					}
					
					byte[] metaRecordbb = prependType(metaType, metabb.array());
					
					FileHandle metafh = tempfh;
//					int metaSlot = 1;
					if(chunkServers.get(0).getEmptySpace(tempfh.getLastChunk())<metaRecordbb.length) {
						metafh = cfs.createNewChunk(tempfh.getDir(), tempfh.getName());
//						metaSlot = 0;
					}
					
					if(chunkServers.get(0).AppendRecord(metafh, metaRecordbb, RecordID) != FSReturnVals.Success) {
						return chunkServers.get(0).AppendRecord(metafh, metaRecordbb, RecordID);
					}
					//adding metaFh was successful, tell replicas to do the same
					
					return chunkServers.get(0).AppendRecord(metafh, metaRecordbb, RecordID);
				}
				if(prevChunkEmptySpace<maxChunkSize) {
					tempfh = cfs.createNewChunk(ofh.getDir(), ofh.getName());
				}
				
				byte[] subPayload = Arrays.copyOfRange(payload, startIndex, endIndex);
				FSReturnVals appfs = chunkServers.get(0).AppendRecord(tempfh, prependType(subType, subPayload), RecordID);
				
				if(appfs != FSReturnVals.Success) {
					System.out.println("Appending subrecord still failed");
					if(appfs == FSReturnVals.Fail) {
						RecordID = null;
					}
					return appfs;
				}
				//subRec was added, now add to replicas
				for(int i = 1; i < 4; i++) {
					chunkServers.get(i).AppendRecord(tempfh, prependType(subType, subPayload), RecordID);
				}
				
				int intChunkHandle = Integer.parseInt(tempfh.getLastChunk());
				subInfoList.add(intChunkHandle);
				subInfoList.add(0);
				
//				if(RecordID.getSlotNumber()==-1) {
//					System.out.println("CHunk:"+RecordID.getChunkHandle());
//					while(true) {}
//				}
				
				startIndex = endIndex;
			}
		}
		
		
		
		
		
		FSReturnVals retVal = chunkServers.get(0).AppendRecord(ofh, prependType(normType, payload), RecordID);
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
		
		else if(retVal == FSReturnVals.Success) {
			for(int i = 1; i < 4; i++) {
				chunkServers.get(i).updateReplicas(ofh);
			}
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
		FileHandle handle = ofh;
		FSReturnVals status = chunkServers.get(0).DeleteRecord(handle, RecordID);
		for(int i = 1; i < chunkServers.size(); i++)
			chunkServers.get(i).updateReplicas(handle);
		return status;
	}

	/**
	 * Reads the first record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadFirstRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadFirstRecord(FileHandle ofh, TinyRec rec){
		for(int i = 1; i < chunkServers.size(); i++)
			chunkServers.get(i).ReadFirstRecord(ofh, rec);
		return chunkServers.get(0).ReadFirstRecord(ofh, rec);
	}

	/**
	 * Reads the last record of the file specified by ofh into payload Returns
	 * BadHandle if ofh is invalid Returns RecDoesNotExist if the file is empty
	 *
	 * Example usage: ReadLastRecord(FH1, tinyRec)
	 */
	public FSReturnVals ReadLastRecord(FileHandle ofh, TinyRec rec){
		for(int i = 1; i < chunkServers.size(); i++)
			chunkServers.get(i).ReadLastRecord(ofh, rec);
		return chunkServers.get(0).ReadLastRecord(ofh, rec);
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
		for(int i = 1; i < chunkServers.size(); i++)
			chunkServers.get(i).ReadNextRecord(ofh, pivot, rec);
		return chunkServers.get(0).ReadNextRecord(ofh, pivot, rec);
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
		for(int i = 1; i < chunkServers.size(); i++)
			chunkServers.get(i).ReadPrevRecord(ofh, pivot, rec);
		return chunkServers.get(0).ReadPrevRecord(ofh, pivot, rec);
	}

}

