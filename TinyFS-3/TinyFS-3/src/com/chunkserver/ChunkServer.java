package com.chunkserver;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
//import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.client.Client;
import com.client.FileHandle;
import com.client.RID;
import com.client.TinyRec;
import com.client.ClientFS.FSReturnVals;
import com.interfaces.ChunkServerInterface;

/**
 * implementation of interfaces at the chunkserver side
 * @author Shahram Ghandeharizadeh
 *
 */

public class ChunkServer implements ChunkServerInterface {
	static String filePath = "csci485/";	//or C:\\newfile.txt
	public final static String ClientConfigFile = "ClientConfig.txt";
	private boolean isRecovering = false;
	
	//Used for the file system
	public static long counter;
	
	//Chunkserver ID
	public int csid;
	
	public static int PayloadSZ = Integer.SIZE/Byte.SIZE;  //Number of bytes in an integer
	public static int CMDlength = Integer.SIZE/Byte.SIZE;  //Number of bytes in an integer  
	
	//Commands recognized by the Server
	public static final int CreateChunkCMD = 101;
	public static final int ReadChunkCMD = 102;
	public static final int WriteChunkCMD = 103;
	
	//Replies provided by the server
	public static final int TRUE = 1;
	public static final int FALSE = 0;
	
	//Record types
	private final int metaType = 20;
	private final int subType = 21;
	private final int normType = 22;
	
	/**
	 * Initialize the chunk server
	 */
	public ChunkServer(){
//		this.csid = id;
		File dir = new File(filePath);
		File[] fs = dir.listFiles();

		if(fs.length == 0){
			counter = 0;
		}else{
			long[] cntrs = new long[fs.length];
			for (int j=0; j < cntrs.length; j++)
				cntrs[j] = Long.valueOf( fs[j].getName() ); 
			
			Arrays.sort(cntrs);
			counter = cntrs[cntrs.length - 1];
		}
	}
	
	/**
	 * Each chunk is corresponding to a file.
	 * Return the chunk handle of the last chunk in the file.
	 */
	public String createChunk() {
		counter += 1;
//		String chunkHandle = String.valueOf(this.csid)+String.valueOf(counter);
		String chunkHandle = String.valueOf(counter);
		initializeChunk(chunkHandle);
		return chunkHandle;
	}
	
	public void initializeChunk(String chunkHandle) {
		ByteBuffer bb = ByteBuffer.allocate(12);
		bb.putInt(0);
		bb.putInt(0);
		bb.putInt(12);
		writeChunk(chunkHandle, bb.array(), 0);
	}
	
	/**
	 * Write the byte array to the chunk at the offset
	 * The byte array size should be no greater than 4KB
	 */
	public boolean writeChunk(String ChunkHandle, byte[] payload, int offset) {
//		System.out.println("Is writing to "+filePath);
		try {
			//If the file corresponding to ChunkHandle does not exist then create it before writing into it
			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.write(payload, 0, payload.length);
			raf.close();
			return true;
		} catch (IOException ex) {
			ex.printStackTrace();
			return false;
		}
	}
	
	public void readFromLog(boolean toLog) {
		System.out.println("readfromlog: "+toLog);
		if(toLog) {
			filePath = "log/";
		}
		else {
			filePath = "csci485/";
		}
	}
	
	public void writeToLog(boolean toLog) {
		System.out.println("wrritetolog: "+toLog);
		if(toLog) {
			filePath = "log/";
		}
		else {
			filePath = "csci485/";
		}
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
		
		System.out.println("Appending type "+getRecordType(payload));
		
		System.out.println("filename = "+ofh.getName());
//		ByteBuffer intBB = ByteBuffer.wrap(payload);
//		int cmdInt = intBB.getInt();
//		System.out.println("CmD type stored ="+cmdInt);
		
		ArrayList<String> chunkList = ofh.getChunkList();
		System.out.println("chunklist size= "+chunkList.size());
		if(chunkList.size()==0) {
			createChunk();
		}
		String chunkHandle = chunkList.get(0);
		if(chunkList.size()>1) {
			chunkHandle = chunkList.get(chunkList.size()-1);
		}
		
		if(payload.length>getEmptySpace(chunkHandle)) {
			System.out.println("Append: Record to long: "+payload.length+" "+getEmptySpace(chunkHandle));
			return FSReturnVals.RecordTooLong;
		}
		int slot = getNumOfSlots(chunkHandle);
		
		int toWriteIndex = getNextAvailableIndex(chunkHandle);
		boolean pass = writeChunk(chunkHandle, payload, toWriteIndex);
//		System.out.println("chunk had "+getNumOfRecords(chunkHandle));
		System.out.println("Writing to:"+filePath+" --> "+chunkHandle+" at "+toWriteIndex+" "+pass);

		if(pass) {
			setOffsetFromSlot(chunkHandle, slot, toWriteIndex);
			//Set RID
			RID rid = new RID(chunkHandle, slot, payload.length);
			RecordID = rid;
			//Manage metadata
			setNumOfRecords(chunkHandle, getNumOfRecords(chunkHandle)+1);
			setNumOfSlots(chunkHandle, getNumOfSlots(chunkHandle)+1);
			setNextAvailableIndex(chunkHandle, getNextAvailableIndex(chunkHandle)+payload.length);
			System.out.println("chunk now has "+getNumOfRecords(chunkHandle));
			return FSReturnVals.Success;
		}
		System.out.println("failed but chunk now has "+getNumOfRecords(chunkHandle));
		return FSReturnVals.Fail;
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
		if (RecordID == null) {
			return FSReturnVals.BadRecID;
		}
		if(!RecordID.checkValid()) {
			return FSReturnVals.BadRecID;
		}
		String chunkHandle = RecordID.getChunkHandle();
		
		
		
		if(getOffsetFromSlot(chunkHandle,RecordID.getSlotNumber()) < 0) {
			return FSReturnVals.RecDoesNotExist;
		}
		//Get first index of record to be deleted
		int toDeleteIndex = getOffsetFromSlot(chunkHandle, RecordID.getSlotNumber());
		
		//Get fist index of bytes to be shifted
		int firstShiftIndex = toDeleteIndex + RecordID.getRecordLength();
		
		//Get last index of bytes to be shifted
		int numOfSlots = getNumOfSlots(chunkHandle);
		int lastShiftIndex = ChunkSize - numOfSlots*4 - 1;
		
		if(lastShiftIndex > firstShiftIndex) {
			byte[] toShiftBA = readChunk(chunkHandle, firstShiftIndex, lastShiftIndex-firstShiftIndex+1);
			writeChunk(chunkHandle, toShiftBA, toDeleteIndex);
			byte[] after = readChunk(chunkHandle, toDeleteIndex, toShiftBA.length);
			if(!Arrays.equals(toShiftBA, after)) {
				System.out.println("array shift failed");
				return FSReturnVals.Fail;
			}
		}
		
		//Update all offsets so that they point to the correct location in the chunk
		int currSlot = RecordID.getSlotNumber();
		setOffsetFromSlot(chunkHandle, currSlot, -1);
		int deletedLength = RecordID.getRecordLength();
		currSlot+=1; //move on to next slot
		while(currSlot < numOfSlots) {
			int offsetVal = getOffsetFromSlot(chunkHandle, currSlot);
			if(offsetVal > 0) {
				int newOffset = offsetVal - deletedLength;
				setOffsetFromSlot(chunkHandle, currSlot, newOffset);
			}
			currSlot += 1; //move on to next slot
		}
		
		//Manage metadata
		setNumOfRecords(chunkHandle, getNumOfRecords(chunkHandle)-1);
		setNextAvailableIndex(chunkHandle, getNextAvailableIndex(chunkHandle)-deletedLength);
		
		RecordID = null;
		
		return FSReturnVals.Success;
	}
	
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
		String chunkHandle = chunkList.get(0);
//		System.out.println("Read First from:"+chunkHandle);
		if(getNumOfRecords(chunkHandle)==0) { // if the chunk is empty
			System.out.println("Chunk "+chunkHandle+" +is empty");
			return FSReturnVals.RecDoesNotExist;
		}

		int firstSlot = getFirstSlotNumber(chunkHandle);
		int firstRecLength = getRecordLength(chunkHandle, firstSlot);	//Get record length of the first valid record, which is the first record

		
		return ReadRecord(ofh, chunkHandle, 12, firstRecLength, firstSlot, rec, true);
//		
//		byte[] firstRec = readChunk(chunkHandle, 12, firstRecLength);
//		int recType = getRecordType(firstRec);
//		RID rid = new RID(chunkHandle, firstSlot, firstRecLength);
//		if(recType==metaType) {	//Meta record, read sub
//			byte[] completeRecord = ReadSubRecord(ofh, firstRec);
//			rec.setPayload(completeRecord);
//			rec.setRID(rid);
//			return FSReturnVals.Success;
//		}
//		else if(recType==subType) {	//Sub record, skip, find next
//			return ReadNextRecord(ofh,rid,rec);
//		}
//		else if(recType==normType) { //Norm record, return
//			rec.setPayload(firstRec);
//			rec.setRID(rid);
//			return FSReturnVals.Success;
//		}
//		else {
//			System.out.println("Undefined record type");
//			return FSReturnVals.Fail;
//		}
	}
	
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
		String chunkHandle = chunkList.get(chunkList.size()-1);
		
		if(getNumOfRecords(chunkHandle)==0) { // if the chunk is empty
			return FSReturnVals.RecDoesNotExist;
		}
		
		int lastSlot = getLastSlotNumber(chunkHandle);
		int lastRecordLen  = getRecordLength(chunkHandle, lastSlot);
		int lastOffset = getOffsetFromSlot(chunkHandle, lastSlot);
		
		
		return ReadRecord(ofh, chunkHandle, lastOffset, lastRecordLen, lastSlot, rec, false);
		
//		byte[] lastRec = readChunk(chunkHandle, lastOffset, recordLen);
//		int recType = getRecordType(lastRec);
//		RID rid = new RID(chunkHandle, lastSlot, recordLen);
//		if(recType==metaType) {	//Meta record, read sub
//			byte[] completeRecord = ReadSubRecord(ofh, lastRec);
//			rec.setPayload(completeRecord);
//			rec.setRID(rid);
//			return FSReturnVals.Success;
//		}
//		else if(recType==subType) {	//Sub record, skip, find next
//			return ReadPrevRecord(ofh,rid,rec);
//		}
//		else if(recType==normType) { //Norm record, return
//			rec.setPayload(lastRec);
//			rec.setRID(rid);
//			return FSReturnVals.Success;
//		}
//		else {
//			System.out.println("Undefined record type");
//			return FSReturnVals.Fail;
//		}
		
//		
//		rec.setPayload(readChunk(chunkHandle, lastOffset, recordLen));
//		rec.setRID(rid);
//		return FSReturnVals.Success;
	}
	
	private FSReturnVals ReadRecord(FileHandle ofh, String chunkHandle, int currOffset,
									int currRecordLen, int currSlot, TinyRec rec, boolean next) {
		byte[] recba = readChunk(chunkHandle, currOffset, currRecordLen);
		int recType = getRecordType(recba);
//		System.out.println("Record is Type: "+recType+"-----------------------");
		RID rid = new RID(chunkHandle, currSlot, currRecordLen);
		if(recType==metaType) {	//Meta record, read sub
			byte[] completeRecord = ReadSubRecord(ofh, recba);
			rec.setPayload(completeRecord);
			rec.setRID(rid);
			return FSReturnVals.Success;
		}
		else if(recType==subType) {	//Sub record, skip, find next/prev based on next
			if(next) {
				FSReturnVals rnfs = ReadNextRecord(ofh,rid,rec);
//				System.out.println("------at subtype readnext, got "+rnfs);
				return rnfs;
			}
			return ReadPrevRecord(ofh,rid,rec);
		}
		else if(recType==normType) { //Norm record, return
			byte[] trimBA = Arrays.copyOfRange(recba, 4, recba.length);
			rec.setPayload(trimBA);
			rec.setRID(rid);
			return FSReturnVals.Success;
		}
		else {
			System.out.println("Undefined record type");
			rec.setPayload(null);
			rec.setRID(null);
			return FSReturnVals.Fail;
		}
	}
	
	public FSReturnVals ReadNextRecord(FileHandle ofh, RID pivot, TinyRec rec) {
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
		int currSlot = pivot.getSlotNumber();
		
		boolean lastChunk = false;
		if(ofh.isLastChunk(chunkHandle)) {
			lastChunk = true;
		}
		
		boolean changedChunk = false;
		if(currSlot==getLastSlotNumber(chunkHandle)) {
			if(lastChunk) {
				rec.setRID(null);
				return FSReturnVals.RecDoesNotExist;
			}
			changedChunk = true;
		}

		
		//Case1: hasn't changed chunk, get next slot
		if(!changedChunk) {
			int nextSlot = getNextValidSlot(chunkHandle, currSlot);
			int nextRecordOffset = getOffsetFromSlot(chunkHandle, nextSlot);
			int nextRecordLen = getRecordLength(chunkHandle, nextSlot);
			
			return ReadRecord(ofh, chunkHandle, nextRecordOffset, nextRecordLen, nextSlot, rec, true);
			
//			byte[] nextRec = readChunk(chunkHandle, recordOffset, recordLen);
//			rec.setPayload(nextRec);
//			RID rid = new RID(chunkHandle, nextSlot, recordLen);
//			rec.setRID(rid);
//			return FSReturnVals.Success;
		}
		//Case2: changed chunk, read first record using first valid slot
		//Note: this chunkhandle is already the new chunkHandle
		else {
			String nextChunkHandle = ofh.getNextChunk(chunkHandle);
			
			if(nextChunkHandle==null) {
				rec.setRID(null);
				return FSReturnVals.RecDoesNotExist;
			}
			int nextNumRecs = getNumOfRecords(nextChunkHandle);
			while(nextNumRecs==0) {
				nextChunkHandle = ofh.getNextChunk(nextChunkHandle);
				if(nextChunkHandle==null) {
					rec.setRID(null);
					return FSReturnVals.RecDoesNotExist;
				}
				nextNumRecs = getNumOfRecords(nextChunkHandle);
			}
			
			System.out.println("change to:"+nextChunkHandle);
			int firstSlotNum = getFirstSlotNumber(nextChunkHandle);
			int recordLen = getRecordLength(nextChunkHandle, firstSlotNum);
			
			return ReadRecord(ofh, nextChunkHandle, 12, recordLen, firstSlotNum, rec, true);
			
			
//			byte[] nextRec = readChunk(nextChunkHandle, 12, recordLen);
//			rec.setPayload(nextRec);
//			RID rid = new RID(nextChunkHandle, firstSlotNum, recordLen);
//			rec.setRID(rid);
//			return FSReturnVals.Success;
		}
	}
	
	public FSReturnVals ReadPrevRecord(FileHandle ofh, RID pivot, TinyRec rec) {
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
		int currSlot = pivot.getSlotNumber();
		
		boolean firstChunk = false;
		if(ofh.isFirstChunk(chunkHandle)) {
			firstChunk = true;
		}
		
		boolean changedChunk = false;
		if(currSlot==getFirstSlotNumber(chunkHandle)) {
			if(firstChunk) {
				rec.setRID(null);
				return FSReturnVals.RecDoesNotExist;
			}
			changedChunk = true;
		}
		
		
		//Case1: hasn't changed chunk, get prev slot
		if(!changedChunk) {
			int prevSlot = getPrevValidSlot(chunkHandle, currSlot);
			int prevRecordOffset = getOffsetFromSlot(chunkHandle, prevSlot);
			int prevRecordLen = getRecordLength(chunkHandle, prevSlot);
			
			return ReadRecord(ofh, chunkHandle, prevRecordOffset, prevRecordLen, prevSlot, rec, false);
			
//			rec.setPayload(readChunk(chunkHandle, recordOffset, recordLen));
//			RID rid = new RID(chunkHandle, prevSlot, recordLen);
//			rec.setRID(rid);
//			return FSReturnVals.Success;
		}
		//Case2: changed chunk, read last record using last valid slot
		//Note: this chunkhandle is already the new chunkHandle
		else {
			
			String prevChunkHandle = ofh.getPrevChunk(chunkHandle);
			if(prevChunkHandle==null) {
				rec.setRID(null);
				return FSReturnVals.RecDoesNotExist;
			}
			int prevNumRecs = getNumOfRecords(prevChunkHandle);
			while(prevNumRecs==0) {
				prevChunkHandle = ofh.getPrevChunk(prevChunkHandle);
				if(prevChunkHandle==null) {
					rec.setRID(null);
					return FSReturnVals.RecDoesNotExist;
				}
				prevNumRecs = getNumOfRecords(prevChunkHandle);
			}
			
			System.out.println("change to:"+prevChunkHandle);
			int lastSlotNum = getLastSlotNumber(prevChunkHandle);
			int lastSlotOffset = getOffsetFromSlot(prevChunkHandle, lastSlotNum);
			int recordLen = getRecordLength(prevChunkHandle, lastSlotNum);
			
			return ReadRecord(ofh, prevChunkHandle, lastSlotOffset, recordLen, lastSlotNum, rec, false);
			
//			rec.setPayload(readChunk(prevChunkHandle, lastSlotOffset, recordLen));
//			RID rid = new RID(prevChunkHandle, lastSlotNum, recordLen);
//			rec.setRID(rid);
//			return FSReturnVals.Success;
		}
	}
	
	
	public byte[] ReadSubRecord(FileHandle ofh, byte[] listba){
		System.out.println("reading subrecords with length = "+listba.length);
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );

		for(int i=4;i<listba.length;) {
			//get chunkhandle
			byte[] chunkba = Arrays.copyOfRange(listba, i, i+4);
			ByteBuffer chunkbb = ByteBuffer.wrap(chunkba);
			String chunkHandle = String.valueOf(chunkbb.getInt());
			System.out.println("chunkHandle ----- is "+chunkHandle);
			i+=4;
			
			//get slotnumber
			byte[] slotba = Arrays.copyOfRange(listba, i, i+4);
			ByteBuffer slotbb = ByteBuffer.wrap(slotba);
			int slotNum = slotbb.getInt();
			
			System.out.println("before fail: slotnum="+slotNum);
			
			try {
				outputStream.write(getRecord(chunkHandle, slotNum));
			} catch (IOException e) {
				e.printStackTrace();
			}
			i+=4;
			
			System.out.println("Sub in "+chunkHandle+" at "+slotNum);
		}
		
		return outputStream.toByteArray();
	}
	
	/**
	 * Helper:
	 * Gets Record from slot
	 */
	private byte[] getRecord(String chunkHandle, int currSlot) {
		int offset = getOffsetFromSlot(chunkHandle, currSlot);
		int length = getRecordLength(chunkHandle, currSlot);
		return readChunk(chunkHandle, offset, length);
	}
	
	/**
	 * Helper:
	 * Gets RecordType
	 */
	private int getRecordType(byte[] payload) {
		byte[] ba = Arrays.copyOfRange(payload, 0, 4);
		ByteBuffer bb = ByteBuffer.wrap(ba);
		int type = bb.getInt();
		return type;
	}
	
	/**
	 * Helper:
	 * Gets the length of the record
	 */
	private int getRecordLength(String chunkHandle, int currSlot) {
		int nextSlotOffset = -1;
		
		if(getLastSlotNumber(chunkHandle)==currSlot) {	//Check if this is the slot of the last record (Not necessarily the last slot)
			nextSlotOffset = getNextAvailableIndex(chunkHandle);
		}
		else {
			int nextSlot = getNextValidSlot(chunkHandle, currSlot);
			nextSlotOffset = getOffsetFromSlot(chunkHandle, nextSlot); //gets offset in second slot
		}
		return nextSlotOffset - getOffsetFromSlot(chunkHandle, currSlot);
	}
	
	/**
	 * Helper:
	 * Returns available empty space
	 */
	public int getEmptySpace(String chunkHandle) {
		int nextIndex = getNextAvailableIndex(chunkHandle);
		int numOfSlots = getNumOfSlots(chunkHandle);
		int lastIndex = ChunkSize - numOfSlots*4 - 4; //-4 because it has to allocate a slot
		System.out.println(chunkHandle+" numRecs = "+getNumOfRecords(chunkHandle)+": nextIndex = "+nextIndex+" numOfSlots = "+numOfSlots+" lastIndex = "+lastIndex);
		return lastIndex-nextIndex;
	}
	
	/**
	 * Helper:
	 * Returns the total number of records in this chunk
	 */
	private int getNumOfRecords(String chunkHandle) {
		byte[] recordNum = readChunk(chunkHandle, 0, 4);
		int numOfRecords = 0;
		if(recordNum!=null) {
			numOfRecords = ByteBuffer.wrap(recordNum).getInt();
		}
//		int numOfRecords = ByteBuffer.wrap(recordNum).getInt();
		return numOfRecords;
	}
	
	/**
	 * Helper:
	 * Sets the total number of records in this chunk
	 */
	private void setNumOfRecords(String chunkHandle, int num) {
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putInt(num);
		writeChunk(chunkHandle, bb.array(), 0);
	}
	
	/**
	 * Helper:
	 * Returns the total number of slots in this chunk
	 */
	private int getNumOfSlots(String chunkHandle) {
		byte[] slotNum = readChunk(chunkHandle, 4, 4);
		int numOfSlots = 0;
		if(slotNum!=null) {
			numOfSlots = ByteBuffer.wrap(slotNum).getInt();
		}
		return numOfSlots;
	}
	
	/**
	 * Helper:
	 * Sets the total number of slots in this chunk
	 */
	private void setNumOfSlots(String chunkHandle, int num) {
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putInt(num);
		writeChunk(chunkHandle, bb.array(), 4);
	}
	
	/**
	 * Helper:
	 * Returns the number of invalid slots
	 */
	private int getNumOfInvalidSlots(String chunkHandle) {
		int count = 0;
		for(int i=0;i<getNumOfSlots(chunkHandle);i++) {
			if(getOffsetFromSlot(chunkHandle, i)<0) {	//Invalid slot
				count += 1;
			}
		}
		return count;
	}
	
	/**
	 * Helper:
	 * Returns the number of valid slots
	 */
	private int getNumOfValidSlots(String chunkHandle) {
		return getNumOfSlots(chunkHandle)-getNumOfInvalidSlots(chunkHandle);
	}
	
	/**
	 * Helper:
	 * Returns last valid slot
	 */
	private int getLastSlotNumber(String chunkHandle) {
		int currSlot = -1;
		for(int i=0;i<getNumOfSlots(chunkHandle);i++) {
			if(getOffsetFromSlot(chunkHandle, i)>0) {	//Valid slot
				currSlot = i;
			}
		}
		return currSlot;
	}
	
	/**
	 * Helper:
	 * Returns first valid slot
	 */
	private int getFirstSlotNumber(String chunkHandle) {
//		int currSlot = -1;
		for(int i=0;i<getNumOfSlots(chunkHandle);i++) {
			if(getOffsetFromSlot(chunkHandle, i)>0) {	//Valid slot
				return i;
			}
		}
		return -1;
	}
	
	/**
	 * Helper:
	 * Get the next available index(offset) in the chunk to store a record
	 */
	private int getNextAvailableIndex(String chunkHandle) {
		byte[] nextIndexBA = readChunk(chunkHandle, 8, 4);
		int nextIndex = ByteBuffer.wrap(nextIndexBA).getInt();
		return nextIndex;
	}
	
	/**
	 * Helper:
	 * Sets the next available index(offset) in the chunk to store a record
	 */
	private void setNextAvailableIndex(String chunkHandle, int num) {
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putInt(num);
		writeChunk(chunkHandle, bb.array(), 8);
	}
	
	/**
	 * Helper:
	 * Input a slot number and returns the offset of the record that is 
	 * stored in this slot
	 */
	private int getOffsetFromSlot(String chunkHandle, int slot) {
		if(slot<0) {
			return -1;
		}
		int offsetIndex = ChunkSize-(slot+1)*4;
		byte[] offsetBA = readChunk(chunkHandle, offsetIndex, 4);
		int offset = ByteBuffer.wrap(offsetBA).getInt();
		return offset;
	}
	
	/**
	 * Helper:
	 * Input a slot number and offset
	 * the function will store that offset in this slot
	 */
	private void setOffsetFromSlot(String chunkHandle, int slot, int offset) {
		int offsetIndex = ChunkSize-(slot+1)*4;
		ByteBuffer bb = ByteBuffer.allocate(4);
		bb.putInt(offset);
		writeChunk(chunkHandle, bb.array(), offsetIndex);
	}
	
	/**
	 * Helper:
	 * Returns the next slot where its stored value is not -1 (Valid slot)
	 */
	private int getNextValidSlot(String chunkHandle, int currSlot) {
		int totalSlots = getNumOfSlots(chunkHandle);
		currSlot += 1;
		while(currSlot<totalSlots) {
			int offset = getOffsetFromSlot(chunkHandle, currSlot);
			if(offset>0) {
				return currSlot;
			}
			currSlot += 1;
		}
		return -1;
	}
	
	/**
	 * Helper:
	 * Returns the prev slot where its stored value is not -1 (Valid slot)
	 */
	private int getPrevValidSlot(String chunkHandle, int currSlot) {
		currSlot -= 1;
		while(currSlot>=0) {
			int offset = getOffsetFromSlot(chunkHandle, currSlot);
			if(offset>0) {
				return currSlot;
			}
			currSlot -= 1;
		}
		return -1;
	}

	
	/**
	 * read the chunk at the specific offset
	 */
	public byte[] readChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		try {
			//If the file for the chunk does not exist the return null
			boolean exists = (new File(filePath + ChunkHandle)).exists();
			if (exists == false) return null;
			
			//File for the chunk exists then go ahead and read it
			byte[] data = new byte[NumberOfBytes];
			RandomAccessFile raf = new RandomAccessFile(filePath + ChunkHandle, "rw");
			raf.seek(offset);
			raf.read(data, 0, NumberOfBytes);
			raf.close();
			return data;
		} catch (IOException ex){
			ex.printStackTrace();
			return null;
		}
	}
	
	public static void ReadAndProcessRequests()
	{
		ChunkServer cs = new ChunkServer();
		
		//Used for communication with the Client via the network
		int ServerPort = 0; //Set to 0 to cause ServerSocket to allocate the port 
		ServerSocket commChanel = null;
		ObjectOutputStream WriteOutput = null;
		ObjectInputStream ReadInput = null;
		
		try {
			//Allocate a port and write it to the config file for the Client to consume
			commChanel = new ServerSocket(ServerPort);
			ServerPort=commChanel.getLocalPort();
			PrintWriter outWrite=new PrintWriter(new FileOutputStream(ClientConfigFile));
			outWrite.println("localhost:"+ServerPort);
			outWrite.close();
		} catch (IOException ex) {
			System.out.println("Error, failed to open a new socket to listen on.");
			ex.printStackTrace();
		}
		
		boolean done = false;
		Socket ClientConnection = null;  //A client's connection to the server

		while (!done){
			try {
				ClientConnection = commChanel.accept();
				ReadInput = new ObjectInputStream(ClientConnection.getInputStream());
				WriteOutput = new ObjectOutputStream(ClientConnection.getOutputStream());
				
				//Use the existing input and output stream as long as the client is connected
				while (!ClientConnection.isClosed()) {
					int payloadsize =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
					if (payloadsize == -1) 
						break;
					int CMD = Client.ReadIntFromInputStream("ChunkServer", ReadInput);
					switch (CMD){
					case CreateChunkCMD:
						String chunkhandle = cs.createChunk();
						byte[] CHinbytes = chunkhandle.getBytes();
						WriteOutput.writeInt(ChunkServer.PayloadSZ + CHinbytes.length);
						WriteOutput.write(CHinbytes);
						WriteOutput.flush();
						break;

					case ReadChunkCMD:
						int offset =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
						int payloadlength =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
						int chunkhandlesize = payloadsize - ChunkServer.PayloadSZ - ChunkServer.CMDlength - (2 * 4);
						if (chunkhandlesize < 0)
							System.out.println("Error in ChunkServer.java, ReadChunkCMD has wrong size.");
						byte[] CHinBytes = Client.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
						String ChunkHandle = (new String(CHinBytes)).toString();
						
						byte[] res = cs.readChunk(ChunkHandle, offset, payloadlength);
						
						if (res == null)
							WriteOutput.writeInt(ChunkServer.PayloadSZ);
						else {
							WriteOutput.writeInt(ChunkServer.PayloadSZ + res.length);
							WriteOutput.write(res);
						}
						WriteOutput.flush();
						break;

					case WriteChunkCMD:
						offset =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
						payloadlength =  Client.ReadIntFromInputStream("ChunkServer", ReadInput);
						byte[] payload = Client.RecvPayload("ChunkServer", ReadInput, payloadlength);
						chunkhandlesize = payloadsize - ChunkServer.PayloadSZ - ChunkServer.CMDlength - (2 * 4) - payloadlength;
						if (chunkhandlesize < 0)
							System.out.println("Error in ChunkServer.java, WritehChunkCMD has wrong size.");
						CHinBytes = Client.RecvPayload("ChunkServer", ReadInput, chunkhandlesize);
						ChunkHandle = (new String(CHinBytes)).toString();

						//Call the writeChunk command
						if (cs.writeChunk(ChunkHandle, payload, offset))
							WriteOutput.writeInt(ChunkServer.TRUE);
						else WriteOutput.writeInt(ChunkServer.FALSE);
						
						WriteOutput.flush();
						break;

					default:
						System.out.println("Error in ChunkServer, specified CMD "+CMD+" is not recognized.");
						break;
					}
				}
			} catch (IOException ex){
				System.out.println("Client Disconnected");
			} finally {
				try {
					if (ClientConnection != null)
						ClientConnection.close();
					if (ReadInput != null)
						ReadInput.close();
					if (WriteOutput != null) WriteOutput.close();
				} catch (IOException fex){
					System.out.println("Error (ChunkServer):  Failed to close either a valid connection or its input/output stream.");
					fex.printStackTrace();
				}
			}
		}
	}

	public static void main(String args[])
	{
		ReadAndProcessRequests();
	}
}
