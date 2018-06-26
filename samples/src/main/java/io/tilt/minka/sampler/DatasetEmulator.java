/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.tilt.minka.sampler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AtomicDouble;

import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.balancer.Balancer.Strategy;

/**
 * Testing facility
 * A simulator that samples a content from a properties with certain keys and values.

  5 =============================================
  6 
  7 # distribution of duties among the pallets
  8 
  9 # SIZE indicates a fixed assignation of duties to pallets, just for test
 10 # WEIGHT indicates an absolute or random weight between a range 0~N and in the same measure than shard capacities
 11 # BALANCER valid values are: 
 12 #       FAIR_WEIGHT     assignation of duties according weights and nodes capacities
 13 #       EVEN_WEIGHT     same duty total weight to each shard regardless of capacities (non asked)
 14 #       EVEN_SIZE       same number of duties to all shards
 15 #       SPILL_OVER      progressive cluster usage by duty agluttination until completion
 16 #       COALESCE        shard agglutination of duties of the same pallet
 17 #       SHUFFLE_ONCE    shuffling assignation of duties
 18 #              NAME    SIZE     WEIGHT   BALANCER
 19 duties.pallets.Manwe = 4:       10:      FAIR_WEIGHT;
 20 
 21 =============================================
 22 
 23 # the different weighing capacities each node will report for each pallet
 24 # (each sampler will lookup in this dataset it's truth table to report to the leader
 25 #                 SHARD  NAME   CAPACITY NAME  CAPACITY NAME   CAPACITY
 26 shards.capacities.9000 = Finwe: 150;     Ewok: 24000;   Manwe: 50;
 27 shards.capacities.9001 = Finwe: 100;     Ewok: 30;      Manwe: 50;
 28 shards.capacities.9002 = Finwe: 250;     Ewok: 20;      Manwe: 110;
 29 shards.capacities.9003 = Finwe: 50;      Ewok: 10;      Manwe: 50;

 */
public class DatasetEmulator implements DummyDataProvider {

	private static final String POWER = "*";

	private static final String FIELD_DELIM = ":";
	private static final String RANGE_DELIM = "~";
	private static final String TERM_DELIM = ";";
	
	// 1d:50:1:C:EVEN_WEIGHT; 1d:/8:188:C:EVEN_WEIGHT;
	private static final String dutyPalletFrmt = "(\\/[0-9]*|[0-9]*):([0-9]*\\~[0-9]*|[0-9]*):([^\\s]+)";
	private static final Pattern dutyPalletFrmtTermPt = Pattern.compile(dutyPalletFrmt);
	
	// 5002:B:*3; 5002:D33:37;  
	private static final String shardCapFrmt = "([^\\s]+):([0-9]*|\\*[0-9]*)";
	private static final Pattern shardCapFrmtTermPt = Pattern.compile(shardCapFrmt);	
	
	private static final String DUTIES_PALLETS = "duties.pallets";
	private static final String SHARDS_CAPACITIES = "shards.capacities";

	private static final String DUTIES_PALLETS_FRMT_EXPLAIN = "bad format on " + DUTIES_PALLETS + 
			": {[fixed int.|/n]:[fixed int.|min~max]:balancer's strategy} but provided: ";

	private static final String SHARD_CAP_FRMT_EXPLAIN = "bad format on " + SHARDS_CAPACITIES + 
			": {palletId:[fixed int.|*n]} but provided:";
	
	private static final Logger logger = LoggerFactory.getLogger(DatasetEmulator.class);
	private static final Random rnd = new Random();
	private Properties prop;
	
	public DatasetEmulator(final Properties prop) throws Exception {
		this.prop = prop;
		System.out.println(prop);
	}
	
	@Override
	public Set<Duty> loadDuties() {
		final Set<Duty> duties = new HashSet<>();
		final AtomicInteger numerator = new AtomicInteger();
		
		for (Object key : prop.keySet()) {
			if (key.toString().startsWith(DUTIES_PALLETS)) {
				final String chunk = prop.getProperty(key.toString())
						.replace(" ", "")
						.replace("\t", "");
				Validate.isTrue(dutyPalletFrmtTermPt.matcher(chunk).find(), DUTIES_PALLETS_FRMT_EXPLAIN + chunk);
				parseDutyFromString(key.toString(), chunk, (duty) -> duties.add(duty), numerator);
			}
		}
		return duties;
	}

	private void parseDutyFromString(final String key, final String chunk, 
			final Consumer<Duty> callback, final AtomicInteger numerator) {

		final String palletName = key.substring(DUTIES_PALLETS.length()+1);
		final String[] parse = chunk.split(FIELD_DELIM);
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
		for (int i = 0; i < size; i++) {
			// this's biased as it's most probably to get the min value when given range is smaller than 0~min
			final long dweight = rangePos > 0 ? Math.max(range[0],rnd.nextInt(range[1])) : weight;
			callback.accept(Duty.builder(
			            String.valueOf(numerator.incrementAndGet()), 
			            String.valueOf(palletName))
			        .with(dweight)
			        .build());
		}
	}

	@Override
	public Set<Pallet> loadPallets() {
		final Set<Pallet> pallets = new HashSet<>();
		for (Object key : prop.keySet()) {
			if (key.toString().startsWith(DUTIES_PALLETS)) {
				final StringTokenizer tok = new StringTokenizer(prop.getProperty(key.toString())
						.replace(" ", "")
						.replace("\t", ""), TERM_DELIM);
				while (tok.hasMoreTokens()) {
					String pbal = tok.nextToken();
					final Strategy strat = Strategy.valueOf(pbal.trim().split(FIELD_DELIM)[2].trim());
					final String palletName = key.toString().substring(DUTIES_PALLETS.length() + 1).trim();
					pallets.add(Pallet.<String>builder(palletName)
							.with(strat.getBalancerMetadata())
							.build());
				}
			}
		}
		return pallets; 
	}
	
	private Set<Pallet> logflags = new HashSet<>();

	private Map<String, Double> capacities = new HashMap<>(); 
	
	@Override
	public double loadShardCapacity(
			final Pallet pallet, 
			final Set<Duty> duties, 
			final String shardIdentifier) {

		final String port = shardIdentifier.split(FIELD_DELIM)[1];
		final String key = port + pallet.getId();
		Double ret = capacities.get(key);
		if (ret == null ) {
			capacities.put(key, new Double(ret = readCapacityFromProperties(pallet, duties, port, shardIdentifier)));
		}
		return ret;
	}

	private double readCapacityFromProperties(
			final Pallet pallet, 
			final Set<Duty> allDuties, 
			final String port, 
			final String shardId) {

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
							allDuties.stream()
									.filter(d -> d.getPalletId().equals(pid))
									.forEach(d -> accumWeight.addAndGet(d.getWeight()));
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
			logger.info("{} Capacity pallet: {} = {}", shardId, pallet.getId(), ret);
			logflags.add(pallet);
		}
		return ret;
	}
}
