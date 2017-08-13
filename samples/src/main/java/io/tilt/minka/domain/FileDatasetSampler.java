
package io.tilt.minka.domain;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * Samples an scenario of pallets and duties using a properties data structure with certain keys/values.
 */
public class FileDatasetSampler {
	
	public static void main(String[] args) throws Exception {
	    // Read a dataset file into a properties
        final FileInputStream fis = new FileInputStream(System.getProperty("dataset.filepath"));
        final Properties prop = new Properties();
        prop.load(fis);
        fis.close();
        
        // Start the Application which starts a Minka server and loads duties and pallets from a sampler 
        final SimpleClientApplication application = new SimpleClientApplication();
        application.start(new DatasetSampler(prop));

		Thread.sleep(60000*10);
	}
	    
}
