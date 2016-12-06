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
package io.tilt.minka.core.follower.impl;

import static io.tilt.minka.domain.ShardEntity.State.CONFIRMED;
import static io.tilt.minka.domain.ShardEntity.State.DANGLING;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.DependencyPlaceholder;
import io.tilt.minka.api.Duty;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.follower.HeartbeatBuilder;
import io.tilt.minka.core.task.LeaderShardContainer;
import io.tilt.minka.domain.AttachedPartition;
import io.tilt.minka.domain.DomainInfo;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.Heartbeat;
import io.tilt.minka.domain.ShardCapacity;
import io.tilt.minka.domain.ShardCapacity.Capacity;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.utils.LogUtils;

/**
 * It keeps sending heartbeats to the leader as long as is alive
 * 
 * @author Cristian Gonzalez
 * @since Nov 17, 2015
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class HeartbeatBuilderImpl implements HeartbeatBuilder {

	private final Logger log = LoggerFactory.getLogger(getClass());

	private final DependencyPlaceholder dependencyPlaceholder;
	private final AttachedPartition partition;
	private final AtomicLong sequence;
	private final Config config;
	
	private DomainInfo domain; 

	public HeartbeatBuilderImpl(final Config config, final DependencyPlaceholder holder, final AttachedPartition partition,
			final LeaderShardContainer leaderShardContainer) {

		super();
		this.config = config;
		this.dependencyPlaceholder = holder;
		this.partition = partition;
		this.sequence = new AtomicLong();
	}

	public Heartbeat build() {
		Set<Duty<?>> reportedDuties;
		try {
			reportedDuties = dependencyPlaceholder.getDelegate().reportCapture();
		} catch (Exception e) {
			log.error("{}: ({}) PartitionDelegate failure", getClass().getSimpleName(), config.getLoggingShardId(), e);
			reportedDuties = new HashSet();
		}

		final List<ShardEntity> hbDuties = new ArrayList<>();
		boolean warning = false;

		// add reported: as confirmed if previously assigned, dangling otherwise. 
		for (final Duty<?> duty : reportedDuties) {
			ShardEntity shardedDuty = partition.forDuty(duty);
			if (shardedDuty != null) {
				final DutyDiff ddiff = new DutyDiff(duty, shardedDuty.getDuty());
				if (!ddiff.hasDiffs()) {
					shardedDuty.registerEvent(CONFIRMED);
				} else {
					log.error("{}: ({}) Delegate reports a different duty than originally attached ! {}", 
						getClass().getSimpleName(), partition.getId(), ddiff.getDiff()); 
				}
			} else {
				shardedDuty = ShardEntity.create(duty);
				// shardedDuty.registerEvent(PartitionEvent.ASSIGN, State.DANGLING);
				shardedDuty.registerEvent(EntityEvent.CREATE, DANGLING);
				log.error("{}: ({}) Reporting a Dangling Duty (by Addition): {}", getClass().getSimpleName(),
						partition.getId(), shardedDuty);
				warning = true;
			}
			hbDuties.add(shardedDuty);
		}

		addAbsents(reportedDuties, hbDuties);

		final Heartbeat hb = Heartbeat.create(hbDuties, warning, partition.getId(), 
				sequence.getAndIncrement(), buildCapacities());
		if (log.isDebugEnabled()) {
			logDebugNicely(hb);
		} else {
			log.debug("{}: ({}) {} SeqID: {}, Duties: {}", getClass().getSimpleName(), hb.getShardId(),
					LogUtils.HB_CHAR, hb.getSequenceId(), hb.getDuties().size());
		}
		return hb;
	}
	public static class DutyDiff {
		private final Duty duty1;
		private final Duty duty2;
		private Map<String, String> diffs;
		public DutyDiff(final Duty d1, final Duty d2) {
			super();
			Validate.notNull(d2);
			Validate.notNull(d1);
			this.duty1 = d1;
			this.duty2 = d2;
		}
		public Map<String, String> getDiff() {
			return diffs;
		}
		public boolean hasDiffs() {
			boolean ret = false;
			if (duty1==null || duty2==null) {
				init();
				diffs.put("duty1", (duty1 == null ? "null" : "notnull"));
				diffs.put("duty1", (duty1 == null ? "null" : "notnull"));
				ret = true;
			} else {
				ret |=! hashAndMethod(duty1, duty2, "duty");
				try {
					ret |=! duty1.getClassType().equals(duty2.getClassType());
					if (!ret && init()) {
						diffs.put("class-types", duty1.getClassType() + "<!=>" + duty2.getClassType());
					}
				} catch (Exception e) {
					init();
					diffs.put("class-types", "duty1.getClassType().equals() fails:" + e.getMessage());
				}
				ret |=! hashAndMethod(duty1.get(), duty2.get(), "payload");
				ret |=! duty1.getPalletId().equals(duty2.getPalletId());
				if (!ret && init()) {
					diffs.put("palletId", duty1.getPalletId() + "<!=>" + duty2.getPalletId());
				}
				ret |=! hashAndMethod(duty1.get(), duty2.get(), "pallet-payload");
			}
			return ret;
		}
		private boolean hashAndMethod(final Object o1 , final Object o2, final String prefix ) {
			boolean ret = true;
			if (o1!=null && o2!=null) {
				ret &= o1.hashCode()==o2.hashCode();
				if (!ret && init()) {
					diffs.put(prefix + "hash-codes", o1.hashCode() + "<!=>" + o2.hashCode());
				}
				try {
					ret &= o1.equals(o2);
					if (!ret && init()) {
						diffs.put(prefix, "equals() fails");
					}
				} catch (Exception e) {
					init();
					diffs.put(prefix, "duty1.get().equals() fails:" + e.getMessage());
				}
			} else {
				init();
				diffs.put(prefix + "-duty1", o1==null ? "null" : "notnull");
				diffs.put(prefix + "-duty2", o2==null ? "null" : "notnull");
			}
			return ret;
		}
		private boolean init() {
			if (diffs==null) {
				diffs = new HashMap<>();
			}
			return true;
		}
	}

	private Map<Pallet<?>, ShardCapacity.Capacity> buildCapacities() {
		final Map<Pallet<?>, ShardCapacity.Capacity> capacities = new HashMap<>();
		if (domain!=null && domain.getDomainPallets()!=null) {
			for (ShardEntity s: domain.getDomainPallets()) {
				double capacity = 0;
				try {
					capacity = dependencyPlaceholder.getDelegate().getTotalCapacity(s.getPallet());
				} catch (Exception e) {
					log.error("{}: ({}) Error ocurred while asking for total capacity on Pallet: {}", getClass().getSimpleName(),
							partition.getId(), s.getPallet(), e);
				} finally {
					final Capacity cap = new Capacity(s.getPallet(), capacity);
					capacities.put(s.getPallet(), cap);
				}
			}
		}
		return capacities;
	}

	private void addAbsents(final Set<Duty<?>> reportedDuties, final List<ShardEntity> hbDuties) {
		// add non-reported: as dangling
		for (final ShardEntity existing : partition.getDuties()) {
			if (!reportedDuties.contains(existing.getEntity())) {
				if (existing.getDuty().isLazyFinalized()) { 
					existing.registerEvent(EntityEvent.REMOVE, ShardEntity.State.FINALIZED);
				} else {
					existing.registerEvent(EntityEvent.REMOVE, DANGLING);
				}
				log.error("{}: ({}) Reporting a Dangling Duty (by Erasure): {}", getClass().getSimpleName(),
						partition.getId(), existing);
				hbDuties.add(existing);
			}
		}
	}

	private void logDebugNicely(final Heartbeat hb) {
		final StringBuilder sb = new StringBuilder();
		List<ShardEntity> sorted = hb.getDuties();
		if (!sorted.isEmpty()) {
			sorted.sort(sorted.get(0));
		}

		long totalWeight = 0;
		for (ShardEntity i : hb.getDuties()) {
			sb.append(i.getDuty().getId()).append("(").append(i.getDuty().getWeight()).append(")").append(", ");
			totalWeight += i.getDuty().getWeight();
		}

		log.debug("{}: ({}) {} SeqID: {}, Duties: {}, Weight: {} = [ {}] {}", getClass().getSimpleName(),
				hb.getShardId(), LogUtils.HB_CHAR, hb.getSequenceId(), hb.getDuties().size(), totalWeight,
				sb.toString(), hb.getDuties().isEmpty() ? "" : "WITH CHANGE");
	}

	@Override
	public void setDomainInfo(final DomainInfo domain) {
		this.domain = domain;
	}
}
