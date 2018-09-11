package io.tilt.minka.core.leader.data;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;

import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.ShardEntity;

@SuppressWarnings({"unchecked", "serial", "rawtypes"})
public class StageRequest implements Serializable {
	
	private static final long serialVersionUID = 7685263765419567211L;
	
	private final ShardEntity entity;
	private CommitState state = CommitState.INIT;
	
	private final Map<EntityEvent.Type, Boolean> sentByState = new LinkedHashMap(2) {{
		put(EntityEvent.Type.ALLOC, false);
		put(EntityEvent.Type.REPLICA, false);
	}};
	
	public StageRequest(
			final ShardEntity entity) {
		this.entity = entity;
	}
	
	public void markSent() {
		sentByState.put(state.getType(), true);
	}
	
	public boolean isSent() {
		return sentByState.get(EntityEvent.Type.ALLOC);
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
}