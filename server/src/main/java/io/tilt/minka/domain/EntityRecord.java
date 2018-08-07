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
package io.tilt.minka.domain;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang.StringUtils.EMPTY;

import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.tilt.minka.api.EntityPayload;

/**
 * Follower's report on {@linkplain ShardedPartition} packed in {@linkplain Heartbeats}.
 * And leader's in-memory version held after distribution. 
 */
public class EntityRecord implements Comparable<EntityRecord>, Comparator<EntityRecord>, EntityPayload, Serializable {

	@JsonIgnore
	private final Logger logger = LoggerFactory.getLogger(getClass());

	private static final long serialVersionUID = 4519763920222729635L;
	private String id;
	private final ShardEntity.Type type;
	private CommitTree commitTree;
	private ShardEntity entity;
	
	private EntityRecord(
			final String id, 
			final ShardEntity.Type type, 
			final CommitTree journal, 
			final ShardEntity entity) {
		this.id = requireNonNull(id);
		this.type = requireNonNull(type);
		this.commitTree = requireNonNull(journal);
		this.entity = entity;
	}
	
	public static EntityRecord fromEntity(final ShardEntity entity, final boolean packit) {
		Validate.notNull(entity);
		return new EntityRecord(
				entity.getEntity().getId(), 
				entity.getType(), 
				entity.getCommitTree(),
				packit? entity : null);
		
	}

	public String getId() {
		return id;
	}	
	
	public ShardEntity getEntity() {
		return entity;
	}
	
	public static String toStringIds(final Collection<EntityRecord> duties) {
		if (duties!=null && duties.size()>0) {
			final StringBuilder sb = new StringBuilder(duties.size()*10);
			duties.forEach(i -> sb.append(i.getId()).append(", "));
			return sb.toString();
		} else {
			return EMPTY;
		}
	}

	@Override
	public String toString() {
		try {
			StringBuilder sb = new StringBuilder(id.length() + 30);

			sb.append(type == ShardEntity.Type.DUTY ? " d:" : "p:").append(id);
			return sb.toString();
		} catch (Exception e) {
			logger.error("tostring", e);
		}
		return null;
	}

	@Override
	public int compareTo(final EntityRecord o) {
		return compare(this, o);
	}

	@JsonIgnore
	public EntityState getLastState() {
		return getCommitTree().getLast().getLastState();
	}

	@JsonIgnore
	public CommitTree getCommitTree() {
        return this.commitTree;
    }
	/*
	@JsonProperty("commitTree")
	private List<String> getJournal_() {
        return this.journal.getStringHistory();
    }
	*/
	public int hashCode() {
		final int prime = 31;
		int res = 1;
		res *= prime + ((type== null ) ? 0 : type.hashCode());
		res *= prime + ((getId()== null ) ? 0 : getId().hashCode());
		return res;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj==null || !(obj instanceof EntityRecord)) {
			return false;
		} else if (obj == this) {
			return true;
		} else {
			final EntityRecord o = (EntityRecord) obj;
			return getType()==o.getType()
					&& getId().equals(o.getId());
		}
	}

	@Override
	public int compare(EntityRecord o1, EntityRecord o2) {
		return o1.getId().compareTo(o2.getId());
	}

	public ShardEntity.Type getType() {
		return type;
	}
	
}
