package io.tilt.minka.broker;

import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.SequenceInputStream;
import java.io.Serializable;
import java.io.StreamCorruptedException;
import java.util.Arrays;
import java.util.Vector;
import java.util.function.BiConsumer;

import org.apache.commons.io.Charsets;

/**
 * Provides a way to transfer large Duty payload contents without allocating memory.
 * Useful for streaming events with binaries like files, images, or HTTP objects.
 * User provides an InputStream fetched by the broker chunk by chunk and transferred 
 * to the user again chunk by chunk to the point of consumption or storage.
 */
public class CustomCoder {
    
	public static class Block {
		private final Serializable message;
		private final InputStream stream;
		public Block(final Serializable message, final InputStream stream) {
			super();
			this.message = message;
			this.stream = stream;
		}
		public InputStream getStream() {
			return stream;
		}
		public Serializable getMessage() {
			return message;
		}
	}
	
	private final static byte TYPE_SIMPLE = 0x0;
	private final static byte TYPE_STREAM = 0x1;

	@SuppressWarnings("resource")
    public static class Decoder {
    
    	private byte[] pbuff = null;
    	private int offset = 0;

    	private ByteArrayOutputStream baos;
    	private PipedOutputStream pipeout;
    	private PipedInputStream pipein;
    	private int type = -1;
    	private Section section = Section.head;
    	
		private final BiConsumer<Serializable, InputStream> biconsumer;
    	
		/** callmeback with the payload and the user stream unfetched every time you find them */
    	public Decoder(final BiConsumer<Serializable, InputStream> consumer) {
    		this.biconsumer = requireNonNull(consumer);
    	}
    	
    	private void drain(final byte[] buffer, final int pidx, final int idx) throws Exception {
			final OutputStream out = section == Section.object ? baos : pipeout;
			if (section.isLimit()) {
				//boolean unread = idx+1<readable;
				try {
				(section == Section.eoo ? baos : pipeout)
					.write(pbuff!=null ? pbuff : buffer, pidx, ((idx-offset)-pidx)+1);
				
				} catch (Exception e) {
					e.printStackTrace();
				}
				if (section == Section.eoo) {
					// prepare for new stream
					pipein = new PipedInputStream(pipeout = new PipedOutputStream(), 2048);
					final Serializable c = convert(baos);
					biconsumer.accept(c, pipein);			
				} else if (section == Section.eob) {
					// notify EOF to user thread
					pipeout.write(0xFFFFFFFF);
					pipeout.flush();
				}
				section = section.next();
			} else { 
				if (pbuff!=null) {
					out.write(pbuff, pidx, pbuff.length-pidx);
					pbuff = null; // reset backup of previous cut when any
				}
				out.write(buffer, pidx, idx-pidx);
				out.flush();
				if (offset > 0) {
					// save current buff: it may be part of a fragmented EO
					pbuff = Arrays.copyOfRange(buffer, pidx, idx-offset);
				}
			}
    	}

    	/**
    	 * Scans the byte array window as fragment/s of a wider message block
    	 * delegating on consumers upon detected apparition of objects and user streams, 
    	 * ../h:1/obj/stream/h:2/object/h1:object/stream/h1:.
    	 */
    	public void decode(final byte[] bytes) throws Exception {
			int idx = 0, pidx = 0;
			byte[] mark = null;
			for (;idx < bytes.length; idx++) {
				if (section == Section.head) {
					pidx = idx+1;
					header(bytes[idx]);
				} else {
					// here only stream/object are possible
					if (mark == null) {
						mark = section.eos();
					}
					if (bytes[idx] == mark[offset]) {
						if (++offset == mark.length) {
							section = section.next();
							// save read bytes from start until end pos to output stream 
							drain(bytes, pidx, idx);
							// save start pos for the next drain
							pidx = idx +1;
							// reset mark index
							offset = 0;
						}
					} else {
						// false mark coincidence: restart 
						offset = 0;
					}
				}
			}
			drain(bytes, pidx, idx);
    	}
   	
    	private void header(final byte head) throws StreamCorruptedException {
			// 1st block
			type = head;
			if (type!=1 && type!=0) {
				throw new StreamCorruptedException("header unknown");
			}
			baos = new ByteArrayOutputStream();
			section = section.next();
    	}

    	private static Serializable convert(final ByteArrayOutputStream baos) throws IOException, ClassNotFoundException {
			final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			final ObjectInputStream ois = new ObjectInputStream(bais);
			return (Serializable)ois.readObject();
    	}    	
    	
    }   
	
	private static final byte[] EOO = "@#EndOfObject$%".getBytes(Charsets.UTF_8);
    private static final byte[] EOB = "@#EndOfBlock$%".getBytes(Charsets.UTF_8);

    private static enum Section {
        
        // type mark
        head,
        // shard entity
        object,
        // end of object
        eoo,
        // user duty input stream
        stream,
        // end of block
        eob
        ;
        public boolean isLimit() {
            return this == eoo || this == eob;
        }
        public byte[] eos() {
            if (this == stream) {
                return EOB;
            } else if (this == object) {
                return EOO;
            } else {
                throw new IllegalArgumentException("curr. section not a body !: " + this);
            }
        }
        public Section next() {
            final int o = this.ordinal();
            if (o+1 == values().length) {
                return head;
            }
            return values()[o+1];
        }
    }
    
    /**
	 * Encoded format for returned input streams:
	 * <pre>
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
	 *  N   block (separator): 	16 bytes for string mark: '@#EndOfBlock$%' (end of user stream N)
	 * --------------------------------------------------------------------------------------
	 *  0..N blocks...
	 * --------------------------------------------------------------------------------------
	 * </pre>
	 */
	public static class Encoder {
		
		/** 
		 * @return an encoded stream with special format containing the companion payload
		 * and the user's stream unfetched and untouched: ready for streaming
		 */
    	public static InputStream encode(final Block block) throws IOException {
    		requireNonNull(block);
    		final ByteArrayOutputStream baos = new ByteArrayOutputStream();    		
    		final Vector<InputStream> vec = new Vector<>(3);
    		
    		// 1st block: what news from the mark ?
			baos.write(block.getStream()==null ? TYPE_SIMPLE : TYPE_STREAM);
    		
    		// 2nd block: companion object
    		final ObjectOutputStream oos = new ObjectOutputStream(baos);
			oos.writeObject(block.getMessage());
    		oos.flush();    		
    		baos.write(Section.object.eos(), 0, Section.object.eos().length);			
			vec.add(new ByteArrayInputStream(baos.toByteArray()));

			// 3rd block: stream
    		if (block.getStream()!=null) {
    			vec.add(block.getStream());
    		}
			// 3rd/4th block: end of stream
    		vec.add(new ByteArrayInputStream(Section.stream.eos()));    			
    		return new SequenceInputStream(vec.elements());
    	}
    	
	}

}