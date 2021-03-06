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

import static org.apache.commons.lang.StringUtils.EMPTY;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.tilt.minka.model.Duty;
import io.tilt.minka.model.Entity;
import io.tilt.minka.model.EntityPayload;
import io.tilt.minka.model.Pallet;
import io.tilt.minka.shard.Shard;
/**
 * Representation of a {@linkplain Duty} selected for an action in a {@linkplain Shard}  
 * 
 * @author Cristian Gonzalez
 * @since Nov 5, 2015
 */
public class ShardEntity implements Comparable<ShardEntity>, Comparator<ShardEntity>, EntityPayload {

	@JsonIgnore
	private final Logger logger = LoggerFactory.getLogger(getClass());

	private static final long serialVersionUID = 4519763920222729635L;
	@JsonIgnore
	private final Entity from;
	private final Type type;
	@JsonIgnore
	private CommitTree tree;
	private EntityPayload userPayload;
	private ShardEntity relatedEntity;
	
	public enum Type {
		DUTY, PALLET
	}

	
	private ShardEntity(final Entity entity, Type type) {
		this.from = entity;
		this.type = type;
		this.tree = new CommitTree();
	}
	
	public static class Builder {
		
		private EntityPayload userPayload;
		private ShardEntity relatedEntity;
		private final Duty duty;
		private final Pallet pallet;
		private ShardEntity from;

		private Builder(final Entity entity) {
			Validate.notNull(entity);
			if (entity instanceof Duty) {
				this.duty = (Duty) entity;
				this.pallet = null;
			} else {
				this.duty = null;
				this.pallet = (Pallet) entity;
			}
		}
		public Builder withRelatedEntity(final ShardEntity relatedEntity) {
			Validate.notNull(relatedEntity);
			this.relatedEntity = relatedEntity;
			return this;
			
		}
		public Builder withPayload(final EntityPayload userPayload) {
			Validate.notNull(userPayload);
			this.userPayload = userPayload;
			return this;
		}
		public ShardEntity build() {
			if (from!=null) {
				final ShardEntity t = new ShardEntity(from.getEntity(), from.getType());
				t.replaceTree(from.getCommitTree());
				t.setUserPayload(from.getUserPayload() !=null ? from.getUserPayload() : userPayload);
				t.setRelatedEntity(from.getRelatedEntity() !=null ? from.getRelatedEntity() : relatedEntity);
				return t;
			} else {
				final ShardEntity ret = new ShardEntity(
						duty == null ? pallet : duty,
						duty == null ? Type.PALLET : Type.DUTY);
				ret.setUserPayload(userPayload);
				ret.setRelatedEntity(relatedEntity);
				return ret;
			}
		}
		public static Builder builderFrom(final ShardEntity entity) {
			final Builder ret = new Builder(entity.getEntity());
			ret.from = entity;
			return ret;
		}
		
		public static Builder builder(final Entity entity) {
			return new Builder(entity);
		}
	}

	private void setRelatedEntity(final ShardEntity entity){
		this.relatedEntity = entity;
	}
	
	@JsonIgnore
	public ShardEntity getRelatedEntity() {
		return this.relatedEntity;
	}
	
	@JsonIgnore
	public Pallet getPallet() {
		if (this.from instanceof Pallet) {
			return (Pallet) this.from;
		}
		throw new IllegalArgumentException("This entity doesnt hold a Pallet !");
	}
	
	@JsonIgnore
	public Entity getEntity() {
		return this.from;

	}

	@JsonProperty("id")
	private String getId_() {
		if (this.from instanceof Duty) {
			return getDuty().getId();
		}
		return "[pallet]";
	}
	
	@JsonIgnore
	public Duty getDuty() {
		if (this.from instanceof Duty) {
			return (Duty) this.from;
		}
		throw new IllegalArgumentException("This entity doesnt hold a Duty !");
	}

	public boolean is(EntityEvent e) {
		return getCommitTree().getLast().getEvent() == e;
	}

	@JsonIgnore
	public EntityEvent getLastEvent() {
		return getCommitTree().getLast().getEvent();
	}
	
	private void setUserPayload(final EntityPayload userPayload) {
		this.userPayload = userPayload;
	}

	@JsonIgnore
	public EntityPayload getUserPayload() {
		return this.userPayload;
	}

	public static String toDutyStringIds(final Collection<Duty> duties) {
		if (duties!=null && duties.size()>0) {
			final StringBuilder sb = new StringBuilder(duties.size() * 10);
			duties.forEach(i -> sb.append(i.getId()).append(", "));
			return sb.toString();
		} else {
			return EMPTY;
		}
	}

	public static String toStringIds(final Collection<ShardEntity> duties) {
		if (duties!=null && duties.size()>0) {
			final StringBuilder sb = new StringBuilder(duties.size()*10);
			duties.forEach(i -> sb.append(i.getEntity().toString()).append(", "));
			return sb.toString();
		} else {
			return EMPTY;
		}
	}

	public static String toStringBrief(final Collection<ShardEntity> duties) {
		if (duties!=null && duties.size()>0) {
			final StringBuilder sb = new StringBuilder(duties.size()*16);
			duties.forEach(i -> sb.append(i.toBrief()).append(", "));
			return sb.toString();
		} else {
			return EMPTY;
		}
	}

	@Override
	public String toString() {
		try {
			final String id = getEntity().toString();

			StringBuilder sb = new StringBuilder(id.length() + 30);

			if (type == Type.DUTY) {
				sb.append("p:").append(getDuty().getPalletId());
			}
			sb.append(type == Type.DUTY ? " d:" : "p:").append(id);
			if (type == Type.DUTY) {
				sb.append(" w:").append(((Duty) getEntity()).getWeight());
			}
			return sb.toString();
		} catch (Exception e) {
			logger.error("tostring", e);
		}
		return null;
	}

	public String toBrief() {
		final String load = type == Type.DUTY ? String.valueOf(this.getDuty().getWeight()) : "";
		final String pid = type == Type.DUTY ? getDuty().getPalletId() : getPallet().getId();
		final String id = getEntity().toString();
		final StringBuilder sb = new StringBuilder(10 + load.length() + id.length() + pid.length());
		sb.append("p:").append(pid).append(" ");
		if (type == Type.DUTY) {
			sb.append("d:").append(id);
			sb.append(" w:").append(load);
		}
		return sb.toString();
	}

	@Override
	public int compareTo(final ShardEntity o) {
		return this.getQualifiedId().compareTo(o.getQualifiedId());
	}

	@JsonIgnore
	public EntityState getLastState() {
		return getCommitTree().getLast().getLastState();
	}

	@JsonIgnore
	public CommitTree getCommitTree() {
        return this.tree;
    }
	
	@JsonProperty("commitTree")
	private List<String> getCommitTree_() {
        return this.tree.getStringHistory();
    }
	
	public void replaceTree(final CommitTree journal) {
	    this.tree = journal;
	}
	
	public static String qualifiedId(final Duty d) {
		return new StringBuilder()
			.append(d.getPalletId())
			.append('-')
			.append(d.getId())
			.toString();
	}
	
	@JsonProperty("qid")
	public String getQualifiedId() {
		final StringBuilder sb = new StringBuilder();
		if (getType()==Type.DUTY) {
			sb.append(((Duty)from).getPalletId()).append('-');
		}
		sb.append(from.getId());
		return sb.toString();
	}

	
	public int hashCode() {
		final int prime = 31;
		int res = 1;
		res *= prime + getQualifiedId().hashCode();
		return res;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj==null || !(obj instanceof ShardEntity)) {
			return false;
		} else if (obj == this) {
			return true;
		} else {
			final ShardEntity o = (ShardEntity) obj;
			return getQualifiedId().equals(o.getQualifiedId());
		}
	}

	@Override
	public int compare(ShardEntity o1, ShardEntity o2) {
		return o1.compareTo(o2);
	}

	@JsonIgnore
	public Type getType() {
		return this.type;
	}

	
	static int compareNulls(final Duty o1, final Duty o2) {
		return o1==null && o1!=o2 ? -1 : o1!=o2 ? 1 : 0;
	}

	static int compareTieBreak(final Duty o1, final Duty o2) {
		int i = o1.getPalletId().compareTo(o2.getPalletId());
		if (i == 0) {
			i = Integer.compare(o1.hashCode(), o2.hashCode());
		}
		return i;
	}

}
