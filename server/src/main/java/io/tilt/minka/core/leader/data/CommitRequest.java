package io.tilt.minka.core.leader.data;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.ShardEntity;

@SuppressWarnings({"unchecked", "serial", "rawtypes"})
public class CommitRequest implements Serializable {
	
	private static final long serialVersionUID = 7685263765419567211L;
	
	private final ShardEntity entity;
	private CommitState state = CommitState.PROCESSING;
	
	private final Map<EntityEvent.Type, Boolean> sentByState = new LinkedHashMap(2) {{
		put(EntityEvent.Type.ALLOC, false);
		put(EntityEvent.Type.REPLICA, false);
	}};
	
	CommitRequest(final ShardEntity entity) {
		this.entity = entity;
	}
	
	public void markSent() {
		sentByState.put(state.getType(), true);
	}
	public boolean isSent() {
		return isSent(EntityEvent.Type.ALLOC);
	}
	public boolean isSent(final EntityEvent.Type type) {
		return sentByState.get(type);
	}
	public CommitState getState() {
		return state;
	}
	public void setState(CommitState state) {
		this.state = state;
	}
	public ShardEntity getEntity() {
		return entity;
	}
	
	@Override
	public int hashCode() {
		return entity.getEntityId().hashCode();
	}
	
	@Override
	public boolean equals(final Object obj) {
		if (obj==null || !(obj instanceof CommitRequest)) {
			return false;
		} else if (obj==this) {
			return true;
		} else {
			final CommitRequest sr = (CommitRequest)obj;
			return sr.getEntity().getEntityId().equals(entity.getEntityId());
		}
	}
	
	@Override
	public String toString() {
		return new StringBuilder()
			.append("e:").append(entity)
			.append(",").append(sentByState)
			.toString();
	}
}