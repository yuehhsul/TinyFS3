package com.chunkserver;

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
import java.util.Arrays;
//import java.util.Arrays;

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
	final static String filePath = "csci485/";	//or C:\\newfile.txt
	public final static String ClientConfigFile = "ClientConfig.txt";
	
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
	
	/**
	 * Initialize the chunk server
	 */
	public ChunkServer(){
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
	
	/**
	 * Appends a record to the open file as specified by ofh Returns BadHandle
	 * if ofh is invalid Returns BadRecID if the specified RID is not null
	 * Returns RecordTooLong if the size of payload exceeds chunksize RID is
	 * null if AppendRecord fails
	 *
	 * Example usage: AppendRecord(FH1, obama, RecID1)
	 */
	public FSReturnVals AppendRecord(String chunkHandle, byte[] payload, RID RecordID) {
		int slot = getNumOfSlots(chunkHandle);
		RecordID.setSlotNumber(slot); //this will be the offset that we want to write to the file
		
		int offset = ChunkSize-(slot+1)*4;
		Boolean pass = writeChunk(chunkHandle, payload, offset);
		if(pass) {
			return FSReturnVals.Success;
		}
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
	public FSReturnVals DeleteRecord(String chunkHandle, RID RecordID) {
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
			byte[] toShiftBA = readChunk(chunkHandle, firstShiftIndex, firstShiftIndex-lastShiftIndex+1);
			writeChunk(chunkHandle, toShiftBA, toDeleteIndex);
		}
		//Update all offsets so that they point to the correct location in the chunk
		int currSlot = RecordID.getRecordLength();
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
		
		return FSReturnVals.Success;
	}
	
	public FSReturnVals ReadFirstRecord(String chunkHandle, TinyRec rec){
		if(getNumOfRecords(chunkHandle)==0) { // if the chunk is empty
			return FSReturnVals.RecDoesNotExist;
		}

		int firstSlot = getFirstSlotNumber(chunkHandle);
		int firstRecLength = getRecordLength(chunkHandle, firstSlot);	//Get record length of the first valid record, which is the first record

		byte[] firstRec = readChunk(chunkHandle, 12, firstRecLength);
		rec.setPayload(firstRec); 
		return FSReturnVals.Success;
	}
	
	public FSReturnVals ReadLastRecord(String chunkHandle , TinyRec rec){
		if(getNumOfRecords(chunkHandle)==0) { // if the chunk is empty
			return FSReturnVals.RecDoesNotExist;
		}
		
		int lastSlot = getLastSlotNumber(chunkHandle);
		int recordLen  = getRecordLength(chunkHandle, lastSlot);
		int lastOffset = getOffsetFromSlot(chunkHandle, lastSlot);
		
		rec.setPayload(readChunk(chunkHandle, lastOffset, recordLen));
		return FSReturnVals.Success;
	}
	
	public FSReturnVals ReadNextRecord(String chunkHandle, TinyRec rec, boolean changedChunk, int currSlot) {
		if(getNumOfRecords(chunkHandle)==0) { // if the chunk is empty
			return FSReturnVals.RecDoesNotExist;
		}
		
		//Case1: hasn't changed chunk, get next slot
		if(!changedChunk) {
			int nextSlot = getNextValidSlot(chunkHandle, currSlot);
			int recordOffset = getOffsetFromSlot(chunkHandle, nextSlot);
			int recordLen = getRecordLength(chunkHandle, nextSlot);
			rec.setPayload(readChunk(chunkHandle, recordOffset, recordLen));
			return FSReturnVals.Success;
		}
		//Case2: changed chunk, read first record using first valid slot
		//Note: this chunkhandle is already the new chunkHandle
		else {
			int firstSlotNum = getFirstSlotNumber(chunkHandle);
			int recordLen = getRecordLength(chunkHandle, firstSlotNum);
			rec.setPayload(readChunk(chunkHandle, 12, recordLen));
			return FSReturnVals.Success;
		}
	}
	
	/**
	 * Helper:
	 * Gets the length of the record
	 */
	private int getRecordLength(String chunkHandle, int currSlot) {
		int nextSlotOffset = -1;
		//Check if there is only one record
		int numOfSlots = getNumOfSlots(chunkHandle);
		if(numOfSlots==1) {
			nextSlotOffset = getNextAvailableIndex(chunkHandle);
			return nextSlotOffset - 12;
		}
		else if(getLastSlotNumber(chunkHandle)==currSlot) {	//Check if this is the slot of the last record (Not necessarily the last slot)
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
	 * Returns the total number of records in this chunk
	 */
	private int getNumOfRecords(String chunkHandle) {
		byte[] recordNum = readChunk(chunkHandle, 0, 4);
		int numOfRecords = ByteBuffer.wrap(recordNum).getInt();
		return numOfRecords;
	}
	
	/**
	 * Helper:
	 * Returns the total number of slots in this chunk
	 */
	private int getNumOfSlots(String chunkHandle) {
		byte[] slotNum = readChunk(chunkHandle, 4, 4);
		int numOfSlots = ByteBuffer.wrap(slotNum).getInt();
		return numOfSlots;
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
			if(getOffsetFromSlot(chunkHandle, i)>0) {	//Invalid slot
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
		int currSlot = -1;
		for(int i=0;i<getNumOfSlots(chunkHandle);i++) {
			if(getOffsetFromSlot(chunkHandle, i)>0) {	//Invalid slot
				return currSlot;
			}
		}
		return currSlot;
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
	 * Input a slot number and returns the offset of the record that is 
	 * stored in this slot
	 */
	private int getOffsetFromSlot(String chunkHandle, int slot) {
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
