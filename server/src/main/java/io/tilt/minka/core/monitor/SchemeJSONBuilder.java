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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
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
	 * Non-Empty only when the current server is the Leader.
	 * @return			a String in json format
	 */
	public String schemeToJson(final boolean detail) {
		final Map<String, Object> m;
		if (detail) {
			final JSONObject j = new JSONObject();
			j.put("commited", buildCommitedState(true));
			j.put("vault", buildVault(detail));
			j.put("replicas", buildReplicas(detail));
			j.put("uncommited", buildDirtyState(detail, scheme.getDirty()));
			m = j.toMap();
		} else {
			m = new LinkedHashMap<>(2);
			final Object det = buildCommitedState(detail);
			m.put("commited", detail ? ((JSONObject)det).toMap() : det);
			m.put("vault", buildVault(detail));
			m.put("replicas", buildReplicas(detail));
			m.put("uncommited", buildDirtyState(detail, scheme.getDirty()));
		}
		return SystemStateMonitor.toJson(m);
	}
	

	@SuppressWarnings({"rawtypes", "unchecked"})
	private Object buildVault(final boolean detail) {
		Validate.notNull(scheme);
		final Map<String, Object> byPalletId = new LinkedHashMap<>();
		final JSONObject js = new JSONObject();
		final Consumer adder;
		if (detail) {
			adder = collectorWithDetail(js);	
		} else {
			adder = collecter(byPalletId);
		}
		scheme.getVault().getAllDuties().forEach(adder);
		return detail ? js : byPalletId;
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
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

	private Map<String, Object> buildReplicas(final boolean detail) {
		Validate.notNull(scheme);
		final Map<String, Object> byPalletId = new LinkedHashMap<>();
		final Consumer<ShardEntity> adder = detail ? collectorWithDetail(byPalletId) : collecter(byPalletId);
		partition.getReplicas().forEach(adder);;
		return byPalletId;
	}

	private Consumer<ShardEntity> collectorWithDetail(final Map<String, Object> byPalletId) {
		final Consumer<ShardEntity> adder = d-> {
			ArrayList<Object> list = (ArrayList) byPalletId.get(d.getDuty().getPalletId());
			if (list==null) {
				byPalletId.put(d.getDuty().getPalletId(), list = new ArrayList<>());
			}
			list.add(d.getCommitTree().toJson());
		};
		return adder;
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
				final JSONObject ks = du.getCommitTree().toJson();
				ks.put("id", du.getDuty().getId());
				array.put(ks);
			} else if (entity instanceof EntityRecord) {
				final EntityRecord er = (EntityRecord)entity;
				final JSONObject ks = er.getCommitTree().toJson();
				ks.put("id", er.getId());
				array.put(ks);
			}
		};
	}

	private Consumer<ShardEntity> collecter(final Map<String, Object> byPalletId) {
		final Consumer<ShardEntity> adder = d-> {
			StringBuilder sb = (StringBuilder) byPalletId.get(d.getDuty().getPalletId());
			if (sb==null) {
				byPalletId.put(d.getDuty().getPalletId(), sb = new StringBuilder());
			}
			sb.append(d.getDuty().getId()).append(',');
		};
		return adder;
	}

	private List<Object> dutyBrief(final Collection<ShardEntity> coll, final boolean detail) {
		List<Object> ret = new ArrayList<>();
		if (!detail) {
			coll.stream()
				.map(d->d.getDuty().getId())
				.forEach(ret::add);
		} else {
			coll.forEach(ret::add);
		}
		return ret;
	}
	
	private Map<String, List<Object>> buildDirtyState(final boolean detail, final DirtyState dirtyState) {
		final Map<String, List<Object>> ret = new LinkedHashMap<>(3);		
		ret.put("crud", dutyBrief(dirtyState.getDutiesCrud(), detail));
		ret.put("dangling", dutyBrief(dirtyState.getDutiesDangling(), detail));
		ret.put("missing", dutyBrief(dirtyState.getDutiesMissing(), detail));
		return ret;
	}

	
}
