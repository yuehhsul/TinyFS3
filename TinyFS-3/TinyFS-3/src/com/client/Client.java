package com.client;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import com.chunkserver.ChunkServer;
import com.interfaces.ClientInterface;

/**
 * implementation of interfaces at the client side
 * @author Shahram Ghandeharizadeh
 *
 */
public class Client implements ClientInterface {
	static int ServerPort = 0;
	static Socket ClientSocket;
	static ObjectOutputStream WriteOutput;
	static ObjectInputStream ReadInput;
	
	public static byte[] RecvPayload(String caller, ObjectInputStream instream, int sz){
		byte[] InputBuff = new byte[sz];
		return InputBuff;
	}
	
	public static int ReadIntFromInputStream(String caller, ObjectInputStream instream){
		
		return -1;
	}
	
	/**
	 * Initialize the client  FileNotFoundException
	 */
	public Client(){
		
	}
	
	/**
	 * Create a chunk at the chunk server from the client side.
	 */
	public String createChunk() {
		return null;
	}
	
	/**
	 * Write a chunk at the chunk server from the client side.
	 */
	public boolean writeChunk(String ChunkHandle, byte[] payload, int offset) {
		return false;
	}
	
	/**
	 * Read a chunk at the chunk server from the client side.
	 */
	public byte[] readChunk(String ChunkHandle, int offset, int NumberOfBytes) {
		return null;
	}

	


}
