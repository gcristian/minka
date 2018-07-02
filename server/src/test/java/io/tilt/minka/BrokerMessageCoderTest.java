package io.tilt.minka;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;
import org.springframework.util.StreamUtils;

import io.tilt.minka.broker.CustomCoder.Block;
import io.tilt.minka.domain.BrokerMessageCoder;

public class BrokerMessageCoderTest {

	@Test
	public void test_encode_and_decode_back_to_start() throws Exception {

		final int[] bufferLengths = new int[] {10, 50, 500, 1024, 2048, 4076, 10*1024};
		final int[] objectLengths = new int[] {1024, 5076, 25*1024};
		final int[] streamLengths = new int[] {64, 512, 3078, 10*2048, 2048*1024};
		
		for (int i = 0 ; i<bufferLengths.length; i++) {
			for ( int j=0; j<objectLengths.length; j++ ) {
				for ( int k=0; k<streamLengths.length; k++ ) {
					System.out.println(String.format("EntityMessageEncoder stream: %s, object: %s, buffer: %s", 
							streamLengths[k], objectLengths[j], bufferLengths[i]));
					test(streamLengths[k], objectLengths[j], bufferLengths[i]);
				}
			}
		}
		
	}
	
	
	private void test(int streamLen, final int objLen, final int buffLen) throws Exception {

		final String streamContent = RandomStringUtils.random(streamLen);
		final String objectCompanion = RandomStringUtils.random(objLen);;
		
		// ====================================================
		final File tmp = File.createTempFile("test-delete-file", "");
		final GZIPOutputStream gos = new GZIPOutputStream(new FileOutputStream(tmp));
		IOUtils.write(streamContent, gos);
		gos.close();
		
		// ----------------------------------------------------
		final GZIPInputStream gis = new GZIPInputStream(new FileInputStream(tmp));
		final InputStream encoded = BrokerMessageCoder.encode(new Block(objectCompanion, gis));
		
		final Block b = BrokerMessageCoder.decode(encoded, buffLen);
		final String decodedCompanion = b.getMessage().toString();
		
		assertEquals(StreamUtils.copyToString(b.getStream(), Charsets.UTF_8), streamContent);
		assertEquals(decodedCompanion, objectCompanion);
		
		gis.close();
		
		tmp.delete();
		
	}
	
}
