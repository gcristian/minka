package io.tilt.minka.domain;

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.SequenceInputStream;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Vector;
import java.util.function.Consumer;

import org.apache.commons.io.Charsets;

import io.netty.handler.codec.CorruptedFrameException;
import io.tilt.minka.broker.CustomCoder.Block;

/**
 * Encoded format for returned input streams:
 * 
 * --------------------------------------------------------------------------------------
 *  1st block (header): 	1 byte for mark that indicates:
 *   
 *  		0: 2nd block is a ByteArrayObjectStream repr. a system entity
 *  		   4th block is the user's InputStream repr. of his own knowledge
 *  
 *  		1: 2nd block is a ByteArrayObjectStream repr. a system entity
 *  		   4th block does not exists
 *  
 * --------------------------------------------------------------------------------------
 *  2nd block (entities): 	N bytes of a ByteArrayObjectStream 
 * --------------------------------------------------------------------------------------
 *  3rd block (separator): 	16 bytes for string mark: '@#EndOfObject$%' (end of object/s)
 * --------------------------------------------------------------------------------------
 *  N   block (payload): 	N bytes of an InputStream
 * --------------------------------------------------------------------------------------
 *  N   block (separator): 	16 bytes for string mark: '@#EndOfStream$%' (end of user stream N)
 * --------------------------------------------------------------------------------------
 *  0..N blocks...
 * --------------------------------------------------------------------------------------
 */
public class BrokerMessageCoder {
		
		private final static byte TYPE_SIMPLE = 0x0;
    	private final static byte TYPE_STREAM = 0x1;
    	private final static byte TYPE_STREAMS = 0x2;

    	private static final byte[] EOO = "@#EndOfObject$%".getBytes(Charsets.UTF_8);
    	private static final byte[] EOS = "@#EndOfStream$%".getBytes(Charsets.UTF_8);
    	private static final byte[] EOB = "@#EndOfBlock$%".getBytes(Charsets.UTF_8);
    	private final static int BUFFER_LENGHT = 2048; //EOO.length * 2;
    	
    	
    	/**
    	 * @param userStreams		the user's stream
    	 * @param companion		traditionally contains the ShardEntities
    	 * @return			an encoded bytes stream with the encoded format
    	 */
    	@Deprecated
    	private static <T>InputStream encodeCollection(
    			final Collection<InputStream> userStreams, 
    			final T companion) throws IOException {
    		requireNonNull(companion);
    		
    		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    		
    		// 1st block
    		final byte type = userStreams == null ? TYPE_SIMPLE : TYPE_STREAMS; 
    		baos.write(type);
    		
    		// 2nd block
    		final ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(companion);
    		oos.flush();
    		    	
    		// 3rd block
    		baos.write(EOO, 0, EOO.length);
    		
    		// 4th block
    		final InputStream ret;
    		if (userStreams!=null) {
    			final Vector<InputStream> v = new Vector<>(userStreams.size()*2);
    			for (final InputStream is: userStreams) {
    				v.add(is);
    				// write separation bytes
    				final ByteArrayOutputStream sep = new ByteArrayOutputStream();
    				sep.write(EOS, 0, EOS.length);
    				v.add(new ByteArrayInputStream(sep.toByteArray()));
    			}
    			final SequenceInputStream sis = new SequenceInputStream(v.elements());
				ret = new SequenceInputStream(new ByteArrayInputStream(baos.toByteArray()), sis);
    		} else {
    			ret = new ByteArrayInputStream(baos.toByteArray());
    		}
    		return ret;
    	}
    	
    	public static <T>InputStream encode(final Block block) throws IOException {
    		requireNonNull(block);
    		final ByteArrayOutputStream baos = new ByteArrayOutputStream();    		
    		final Vector<InputStream> vec = new Vector<>();
    		
    		// 1st block: what news from the mark ?
			baos.write(block.getStream()==null ? TYPE_SIMPLE : TYPE_STREAM);
    		
    		// 2nd block: companion object
    		final ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(block.getMessage());
    		oos.flush();    		
    		baos.write(EOO, 0, EOO.length);			
			vec.add(new ByteArrayInputStream(baos.toByteArray()));

			// 3rd block: stream
    		if (block.getStream()!=null) {
    			vec.add(block.getStream());
    		}
			// 3rd/4th block: end of stream
    		vec.add(endOfBlock());    			
    		return new SequenceInputStream(vec.elements());
    	}
    	
    	private static InputStream endOfBlock() {
    		// 5th end of stream
			final ByteArrayOutputStream eos = new ByteArrayOutputStream();
			eos.write(EOB, 0, EOB.length);
    		return new ByteArrayInputStream(eos.toByteArray());
    	}
 
    	/**
    	 * Reads an encoded {@linkplain InputStream} and calls back the companion consumer
    	 * with the object found, then keeps reading the origin: calling back
    	 * the passed consumer between EOF of each stream encoded.
    	 * 
    	 * Caller is free to match each stream with companion object details. 
    	 * 
    	 * @param	origin		an InputStream encoded with {@linkplain EntityMessageCode}
    	 * @param	streams		a consumer to pass each stream encoded found in origin
    	 * @param	companion	a consumer to pass the element encoded at the beginning of origin
    	 */
    	public static Block decode(final InputStream origin) throws IOException, ClassNotFoundException {
    		return decode(origin, BUFFER_LENGHT);
    	}
    	// 
    	// ../h:1/obj/stream/h:2/object/h1:object/stream/h1:.
    	public static Block decode(final InputStream origin, int bufferSize) throws IOException, ClassNotFoundException {
    		
    		// 1st block
			int header = origin.read();

			if (header == TYPE_STREAM) {
				// 2nd block
				
				bufferSize = Math.max(BUFFER_LENGHT, bufferSize);
				final byte[] buff = new byte[bufferSize];
				
				final ByteArrayOutputStream baoss = new ByteArrayOutputStream();
				boolean sepFound = false;
				int offset = 0;

				boolean unread = false;
				byte[] prevbuff = null;
				int idx = 0;
				int read = 0;

				while ((read=origin.read(buff, 0, bufferSize))>0) {
					// look for 3rd block
					idx = 0;
					for (;idx<read; idx++) {
						if (buff[idx]==EOO[offset]) {
							if (++offset==EOO.length) {
								sepFound = true;
								break;
							}
						} else {
							offset = 0;
						}
					}
					if (sepFound) {
						// read til the mark and trigger unread so the user stream is saved
						if (prevbuff!=null) {
							int oldlen = bufferSize - (EOO.length - (idx+1));
							baoss.write(prevbuff, 0, oldlen);
						} else {
							baoss.write(buff, 0, (idx+1)-EOO.length);
						}
						unread |= idx+1<read;
						break;
					}
					if (prevbuff!=null) {
						baoss.write(prevbuff, 0, prevbuff.length);
						prevbuff = null;
					}
					if (offset == 0) {
						// all what's left is an object
						baoss.write(buff, 0, buff.length);
						prevbuff = null;
					} else if (offset>0) {
						prevbuff = Arrays.copyOf(buff, buff.length);
					} else {
						prevbuff = null; // reset backup of previous cut when any
					}
				}
				if (!sepFound) {
					throw new StreamCorruptedException("separator block not found in piped stream");
				}

				final ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baoss.toByteArray()));
				
				// 4th block
				final Serializable serie = (Serializable)ois.readObject();
				if (unread) {
					final ByteArrayInputStream remaining = new ByteArrayInputStream(buff, idx, buff.length-(idx+1));
					return new Block(serie, new SequenceInputStream(remaining, origin));
				} else {
					return new Block(serie, origin);
				}
			} else if (header == TYPE_SIMPLE){
			    final Serializable x = (Serializable) new ObjectInputStream(origin).readObject();
				return new Block(x, null);
			} else {
				throw new StreamCorruptedException("stream not encoded with BrokerMessageCoder");
			}
    	}
    	
    	
    	public void decodes(
    			final InputStream origin, 
    			final byte[] eos, 
    			final Consumer<Byte> byter) throws IOException {
    		
    		final byte[] buff = new byte[BUFFER_LENGHT];
			
    		/// ESTE METODO NO SIRVE CON EL BAOS xq no voy a escribir un BAOS
    		// tengo q escribir una pipa q alguien lea como InputStream
    		// osea aprovecharia todo el mecanismo de parseo y hallazgo
    		// pero en vez de baos.write... seria algo NO-EN-MEMORIA como el BAOS\
    		/// seria algo del tipo PIPED... sale byte, cliente lee byte...
    		// sale -1, cliente sabe q cerro ESE IS, y viene otro...
    		
    		
			final ByteArrayOutputStream baoss = new ByteArrayOutputStream();
			boolean sepFound = false;
			int offset = 0;

			
			
			boolean unread = false;
			byte[] prevbuff = null;
			int idx = 0;
			int read = 0;
			while ((read=origin.read(buff, 0, BUFFER_LENGHT))>0) {
				// look for 3rd block
				idx = 0;
				for (;idx<read; idx++) {
					if (buff[idx]==eos[offset]) {
						if (++offset==eos.length) {
							sepFound = true;
							break;
						}
					} else {
						offset = 0;
					}
				}
				if (sepFound) {
					// read til the mark and trigger unread so the user stream is saved
					if (prevbuff!=null) {
						int oldlen = BUFFER_LENGHT - (eos.length - (idx+1));
						baoss.write(prevbuff, 0, oldlen);
					} else {
						baoss.write(buff, 0, (idx+1)-eos.length);
					}
					unread |= idx+1<read;
					break;
				}
				if (prevbuff!=null) {
					baoss.write(prevbuff, 0, prevbuff.length);
					prevbuff = null;
				}
				if (offset == 0) {
					// all what's left is an object
					baoss.write(buff, 0, buff.length);
					prevbuff = null;
				} else if (offset>0) {
					prevbuff = Arrays.copyOf(buff, buff.length);
				} else {
					prevbuff = null; // reset backup of previous cut when any
				}
			}
			if (!sepFound) {
				throw new CorruptedFrameException("separator block not found in piped stream");
			}
    	}
    	
    }