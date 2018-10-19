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
package io.tilt.minka.core.monitor;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.lang.Validate;
import org.json.JSONArray;
import org.json.JSONObject;

import io.tilt.minka.core.leader.data.DirtyState;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.domain.EntityRecord;
import io.tilt.minka.domain.ShardEntity;
import io.tilt.minka.domain.ShardedPartition;

@SuppressWarnings({"rawtypes", "unchecked"})
public class SchemeJSONBuilder {
	
	private final Scheme scheme;
	private final ShardedPartition partition;

	public SchemeJSONBuilder(
			final Scheme scheme,
			final ShardedPartition partition) {
		super();
		
		this.scheme = requireNonNull(scheme);
		this.partition = requireNonNull(partition);		
	}

	/**
	 * 
	 * <p>
	 * Non-Empty only when the current server is the LeaderBootstrap.
	 * @return			a String in json format
	 */
	public String schemeToJson(final boolean detail) {
		final Map<String, Object> m;
		if (detail) {
			final JSONObject j = new JSONObject();
			j.put("committed", buildCommitedState(true));
			j.put("vault", dutyBrief(scheme.getVault().getAllDuties(), detail));
			j.put("replicas", dutyBrief(partition.getReplicas(), detail));
			j.put("uncommitted", buildDirtyState(detail, scheme.getDirty()));
			m = j.toMap();
		} else {
			m = new LinkedHashMap<>(2);
			// ((JSONObject)det).toMap()
			m.put("committed", buildCommitedState(false));
			m.put("vault", dutyBrief(scheme.getVault().getAllDuties(), detail));
			m.put("replicas", dutyBrief(partition.getReplicas(), detail));
			m.put("uncommitted", buildDirtyState(detail, scheme.getDirty()));
		}
		return SystemStateMonitor.toJson(m);
	}

	private Object buildCommitedState(final boolean detail) {
		Validate.notNull(scheme);
		final Map<String, Object> byPalletId = new LinkedHashMap<>();
		final JSONObject js = new JSONObject();
		final Consumer adder;
		if (detail) {
			adder = collectorWithDetail(js);	
		} else {
			adder = collecter(byPalletId);
		}
				
		scheme.getCommitedState().findDuties(adder);
		return detail ? js : byPalletId;
	}
	
	private Map<String, Object> buildDirtyState(final boolean detail, final DirtyState dirtyState) {
		final Map<String, Object> ret = new LinkedHashMap<>(3);		
		ret.put("crud", dutyBrief(dirtyState.getDutiesCrud(), detail));
		ret.put("dangling", dutyBrief(dirtyState.getDutiesDangling(), detail));
		ret.put("missing", dutyBrief(dirtyState.getDutiesMissing(), detail));
		return ret;
	}
	
	private Consumer<Object> collectorWithDetail(final JSONObject js) {
		return entity-> {
			final String pid;
			
			if (entity instanceof ShardEntity) {
				pid = ((ShardEntity)entity).getDuty().getPalletId();
			} else if (entity instanceof EntityRecord) {
				pid = ((EntityRecord)entity).getPalletId();
			} else {
				throw new IllegalArgumentException("not an entity");
			}
			
			JSONArray array = js.has(pid) ? js.getJSONArray(pid) : null;
			if (array==null) {
				js.put(pid, array = new JSONArray());
			}
			
			if (entity instanceof ShardEntity) {
				ShardEntity du = ((ShardEntity)entity);
				final JSONObject ks = CommitTreeConverter.toJson(du.getCommitTree());
				ks.put("id", du.getDuty().getId());
				array.put(ks);
			} else if (entity instanceof EntityRecord) {
				final EntityRecord er = (EntityRecord)entity;
				final JSONObject ks = CommitTreeConverter.toJson(er.getCommitTree());
				ks.put("id", er.getId());
				array.put(ks);
			}
		};
	}
	
	private Consumer<Object> collecter(final Map<String, Object> byPalletId) {
		return d-> {
			final String pid;
			if (d instanceof ShardEntity) {
				pid = ((ShardEntity)d).getDuty().getPalletId();
			} else if (d instanceof EntityRecord) {
				pid = ((EntityRecord)d).getPalletId();
			} else {
				throw new IllegalArgumentException("not an entity");
			}
			StringBuilder sb = (StringBuilder) byPalletId.get(pid);
			if (sb==null) {
				byPalletId.put(pid, sb = new StringBuilder());
			}
			if (d instanceof ShardEntity) {
				sb.append(((ShardEntity)d).getDuty().getId()).append(',');
			} else if (d instanceof EntityRecord) {
				sb.append(((EntityRecord)d).getId()).append(',');
			}
		};
	}

	private Object dutyBrief(final Collection coll, final boolean detail) {
		Validate.notNull(scheme);
		final Map<String, Object> byPalletId = new LinkedHashMap<>();
		final JSONObject js = new JSONObject();
		final Consumer adder;
		if (detail) {
			adder = collectorWithDetail(js);	
		} else {
			adder = collecter(byPalletId);
		}				
		return detail ? js : byPalletId;
	}

}
