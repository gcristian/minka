package io.tilt.minka.core.leader.data;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

import io.tilt.minka.api.Duty;

/**
 * A storage facility to save user streams to disk, before distribution takes place. 
 * Then transfer occurrs as an attach, where the input stream is fetched 
 * from disk at the same time it's read by the receiving follower and the user's delegate.
 * Streams are temporarily saved, they dont require to survive the current session.
 */
public class EntityRepository {

	private final Logger logger = LoggerFactory.getLogger(getClass());
	private final static String PREFIX = "stream-";
	private final static String TMP_DIR = "minka";
	
	private final Map<String, String> map = new HashMap<>();
	
	public boolean exists(final Duty duty) {
		return map.containsKey(duty.getId());
	}
	
	public InputStream upstream(final Duty duty) {
		if (!exists(duty)) {
			return new ByteArrayInputStream("nothing-to-do".getBytes(Charsets.UTF_8));
		}
		final String fname = map.get(duty.getId());
		try {
			final FileInputStream fis = new FileInputStream(fname);
			fis.getFD().sync();
			return fis;
		} catch (Exception e) {
			logger.error("{}: Cannot read: {}, file: {}", getClass().getSimpleName(), duty.getId(), fname, e);
		}
		return null;
	}
	
	public void remove(final Duty entity) {
		final String fname = map.get(entity.getId());
		FileUtils.deleteQuietly(new File(map.get(fname)));
	}
	
	public void downstream(final Duty entity, final InputStream stream) {
	    
		String fname = "";
		
		try {
		    int read = 0 ;
		    final File f;
		    final String fid = entity.getId();
		    
		    final String tmpdir = FileUtils.getTempDirectoryPath();
		    
		    if (tmpdir==null) {
		    	f = File.createTempFile(PREFIX, fid);
		    } else {
		    	File dir = new File(tmpdir + File.pathSeparator + PREFIX);
		    	dir.mkdirs();
		    	f = File.createTempFile(entity.getId(), "", dir);
		    }
		    f.deleteOnExit();
            fname = f.getAbsolutePath();
            
		    final FileOutputStream fos = new FileOutputStream(f);
            while((read = stream.read())>-1) {
                fos.write(read);
            }
            fos.flush();
            fos.getFD().sync();
            
			map.put(fid, fname);
            fos.close();
        } catch (IOException e) {
        	logger.error("{}: Cannot write: {}, file: {}", getClass().getSimpleName(), entity.getId(), fname, e);
        }
	}
}
