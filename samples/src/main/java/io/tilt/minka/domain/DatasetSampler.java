
package io.tilt.minka.domain;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AtomicDouble;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.DutyBuilder;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.api.PalletBuilder;
import io.tilt.minka.core.leader.distributor.Balancer.Strategy;

/**
 * README: in this file I can define a parametrized dataset to load minka entities
 * and automate correctness validations on distribution and balance algorithms
 * about what's to be expected after time when all shards are online and stable
 * we can define our own to simulate a real scenario and check how minka works
 * saving lot of tedious programmatic lines of code
 * 
 * HELP: duties are inputs to functions, whether short-term, long-term, static or dynamic
 * pallets represent functions running in all machines, a facility to group duties
 * shards represent machines holding portions of the duty universe
 * weight value is abstract but in a real scenario should link to the physical resource used by the pallet
 * capacity is in relation of each shard's physical resource total capability for every pallet
 * 
 * SAMPLE: for instance, pallet A may make use of hardisk, B of network bandwidth, and C of cpu threads
 * minka is so abstract that lets the host application articulate the domain model to fit their needs
 * achieving any already legacy or particular combination of resource exhaustion
 * 
 * small DSL: terms are ; separated, and params within terms are : separated,
 * / is division of a whole, and * is multiplo of a whole
 * 
 * SAMPLE FILE
 * 
 * duties.size = 500
 * 
 * # number of shards to boot infered by ports
 * # NOTICE: we must shoot as many shards as defined in shard.ports, with those ports
 * # format: {{shard1's HTTP port};{...}}
 * shards.ports = 5001;5002
 * 
 * # pallet balancers, distribution of duties on pallets and duty weights  
 * # format: 
 * # 	{{pallet id}:
 * # 	 {slice of duties.size or fixed integer}:
 * #	 {weight = min~max random range or fixed integer}:
 * #	 {balancer on class Balancers.Strategy}
 * #		;
 * #     {...}}
 * duties.pallets = A:/2:50~100:A:EVEN_WEIGHT; B:200:500:B:SPILLOVER; C:50:1:C:ROUND_ROBIN
 * 
 * # assignation of capacity on shards for every pallet loaded
 * # format: {{shard}:{pallet}:{mutiple of duties.pallet:weight or fixed integer};{...}}
 * shards.capacities = 5001:A:5000; 5001:B:*5; 5002:A:*4; 5002:B:*3; 5003:A:7000; 5003:B:7000
 *
 */
public class DatasetSampler extends AbstractMappingEventsApp {

	private static final String POWER = "*";

	private static final String FIELD_DELIM = ":";
	private static final String RANGE_DELIM = "~";
	private static final String TERM_DELIM = ";";

	private static final String sep = "(;[^\\s]|;)";
	
	// 1d:50:1:C:EVEN_WEIGHT; 1d:/8:188:C:EVEN_WEIGHT;
	private static final String dutyPalletFrmt = "(\\/[0-9]*|[0-9]*):([0-9]*\\~[0-9]*|[0-9]*):([^\\s]+)";
	private static final Pattern dutyPalletFrmtTermPt = Pattern.compile(dutyPalletFrmt);
	private static final Pattern dutyPalletFrmtPt = Pattern.compile(dutyPalletFrmt + sep);
	
	// 5002:B:*3; 5002:D33:37;  
	private static final String shardCapFrmt = "([^\\s]+):([0-9]*|\\*[0-9]*)";
	private static final Pattern shardCapFrmtTermPt = Pattern.compile(shardCapFrmt);	
	private static final Pattern shardCapFrmtPt = Pattern.compile(shardCapFrmt + sep);
	
	private static final String DUTIES_SIZE = "duties.size";	
	private static final String DUTIES_PALLETS = "duties.pallets";
	private static final String SHARDS_CAPACITIES = "shards.capacities";

	private static final String DUTIES_PALLETS_FRMT_EXPLAIN = "bad format on " + DUTIES_PALLETS + 
			": {[fixed int.|/n]:[fixed int.|min~max]:balancer's strategy} but provided: ";

	private static final String SHARD_CAP_FRMT_EXPLAIN = "bad format on " + SHARDS_CAPACITIES + 
			": {palletId:[fixed int.|*n]} but provided:";
	
	private static final Logger logger = LoggerFactory.getLogger(DatasetSampler.class);
	private static final Random rnd = new Random();
	private Properties prop;
	
	public static void main(String[] args) throws Exception {
		new DatasetSampler().startDemo();
		Thread.sleep(60000*10);
	}
	public DatasetSampler() throws Exception {
		super();
	}

	public void init() throws Exception {
		if (this.prop == null) {
			final String datasetFilepath = System.getProperty("dataset.filepath");
			this.prop = new Properties();
			final FileInputStream fis = new FileInputStream(datasetFilepath);
			prop.load(fis);
			fis.close();
			
			//final String dp = prop.getProperty(DUTIES_PALLETS);
			//Validate.isTrue(dutyPalletFrmtPt.matcher(dp).find(), DUTIES_PALLETS_FRMT_EXPLAIN + dp);
			//final String sc = prop.getProperty(SHARDS_CAPACITIES);
			//Validate.isTrue(shardCapFrmtPt.matcher(sc).find(), SHARD_CAP_FRMT_EXPLAIN + sc);
		}
	}
	
	@Override
	public Set<Duty<String>> buildDuties() throws Exception {
		init();
		final Set<Duty<String>> duties = new HashSet<>();
		int dutyId = 0;
		
		for (Object key: prop.keySet()) {
		    if (key.toString().startsWith(DUTIES_PALLETS )) {
		        final String chunk = prop.getProperty(key.toString())
		                .replace(" ", "")
		                .replace("\t", "");
		        Validate.isTrue(dutyPalletFrmtTermPt.matcher(chunk).find(), DUTIES_PALLETS_FRMT_EXPLAIN + chunk);
		        final String palletName = key.toString().substring(DUTIES_PALLETS.length()+1);
		        dutyId = parseDutyDefinitionAndBuild(duties, dutyId, chunk, palletName);
		    }
		}		
		return duties;
	}

	private int parseDutyDefinitionAndBuild(
	        final Set<Duty<String>> duties, 
	        int dutyNumerator, 
	        final String dpal, 
	        final String palletName) {
		final String[] parse = dpal.split(FIELD_DELIM);
		final String sliceStr = parse[0].trim();
		final String weightStr = parse[1].trim();
		final int size =  Integer.parseInt(sliceStr);
		final int rangePos = weightStr.indexOf(RANGE_DELIM);
		int[] range = null;
		int weight = 0;
		if (rangePos > 0) {
			range = new int[] { 
			        Integer.parseInt(weightStr.trim().split(RANGE_DELIM)[0].trim()), 
					Integer.parseInt(weightStr.split(RANGE_DELIM)[1].trim()) };
		} else {
			weight = Integer.parseInt(weightStr);
		}
		logger.info("Building {} duties for pallet: {}", size, palletName);
		for (int i = 0; i < size; i++, dutyNumerator++) {
			// this's biased as it's most probably to get the min value when given range is smaller than 0~min
			final long dweight = rangePos > 0 ? Math.max(range[0],rnd.nextInt(range[1])) : weight;
			duties.add(DutyBuilder.<String>builder(
			            String.valueOf(dutyNumerator), 
			            String.valueOf(palletName))
			        .with(dweight)
			        .build());
		}
		return dutyNumerator;
	}

	@Override
	public Set<Pallet<String>> buildPallets() throws Exception {
		init();
		final Set<Pallet<String>> pallets = new HashSet<>();
		for (Object key: prop.keySet()) {
		    if (key.toString().startsWith(DUTIES_PALLETS)) {  
        		final StringTokenizer tok = new StringTokenizer(prop.getProperty(key.toString())
        		        .replace(" ", "")
        		        .replace("\t", ""), TERM_DELIM);
        		while (tok.hasMoreTokens()) {
        			String pbal = tok.nextToken();	
        			final Strategy strat = Strategy.valueOf(pbal.trim().split(FIELD_DELIM)[2].trim());
        			final String palletName = key.toString().substring(DUTIES_PALLETS.length()+1).trim();
        			pallets.add(PalletBuilder.<String>builder(palletName)
        					.with(strat.getBalancerMetadata())
        					.build());
        		}
		    }
		}
		return pallets; 
	}
	
	private Set<Pallet<?>> logflags = new HashSet<>();

	private Map<String, Double> capacities = new HashMap<>(); 
	
	@Override
	public double getTotalCapacity(Pallet<String> pallet) {
		final String port = getMinkaClient().getShardIdentity().split(FIELD_DELIM)[1];
		final String key = port + pallet.getId();
		Double ret = capacities.get(key);
		if (ret == null ) {
			capacities.put(key, new Double(ret = retrieveCapacity(pallet, port)));
		}
		return ret;
	}

	private double retrieveCapacity(final Pallet<?> pallet, final String port) {
		double ret = 0;
		for (Object key: prop.keySet()) {
		    if (key.toString().startsWith(SHARDS_CAPACITIES)) {
		        final String portStr = key.toString().substring(SHARDS_CAPACITIES.length()+1);
        		final StringTokenizer tok = new StringTokenizer(prop.getProperty(key.toString()), TERM_DELIM);
        		while (tok.hasMoreTokens()) {
        			final String cap = tok.nextToken();
        			if (cap.trim().isEmpty()) {
        				continue;
        			}
        			Validate.isTrue(shardCapFrmtTermPt.matcher(cap).find(), SHARD_CAP_FRMT_EXPLAIN + cap);
        			final String[] capParse = cap.split(FIELD_DELIM);
        			final String pid = capParse[0].trim();
        			final String capacity = capParse[1].trim();
        			
        			if (portStr.equals(port) && pid.equals(pallet.getId())) {
        				if (capacity.startsWith(POWER)) {
        					AtomicDouble accumWeight = new AtomicDouble(0);
        					getAllOriginalDuties().stream().filter(d->d.getPalletId().equals(pid))
        						.forEach(d->accumWeight.addAndGet(d.getWeight()));
        					ret = accumWeight.get() * Double.parseDouble(capacity.substring(1));
        				} else {
        					ret = Double.parseDouble(capacity);
        				}
        				break;
        			}
        		}
		    }
		}
		if (!logflags.contains(pallet)) {
			logger.info("{} Capacity pallet: {} = {}", super.getMinkaClient().getShardIdentity(), pallet.getId(), ret);
			logflags.add(pallet);
		}
		return ret;
	}
}
