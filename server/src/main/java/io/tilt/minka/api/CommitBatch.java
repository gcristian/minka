package io.tilt.minka.api;

import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import io.tilt.minka.domain.ShardEntity;

public abstract class CommitBatch implements Serializable {
	
	private static final long serialVersionUID = 3276857135029280113L;		
	private final String id;
	
	CommitBatch(final String id) {
		this.id = id;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (obj==null || !(obj instanceof CommitBatch)) {
			return false;
		} else if (obj==this) {
			return true;
		} else {
			final CommitBatch cr = (CommitBatch)obj;
			return cr.getClass().equals(getClass())
					&& cr.getId().equals(this.id);
		}
	}
	@Override
	public int hashCode() {
		return (id.hashCode() + "-" + getClass().hashCode()).hashCode();
	}
	public String getId() {
		return id;
	}
	@Override
	public String toString() {
		return getClass().getSimpleName() + "-" + id;
	}
	

	/**
	 * Internal summary of a single operation done to a number of entities
	 * thru the Client instance when its shard is not a Leader.
	 * Deviated from the follower shard to the leader one. 
	 */
	public static class CommitBatchRequest extends CommitBatch {
		
		private static final long serialVersionUID = 5457510210525879884L;
		
		private final List<ShardEntity> entities;
		private final boolean respondBack;
		
		CommitBatchRequest(final List<ShardEntity> entities, final boolean respondBack) {
			super(UUID.randomUUID().toString());
			this.entities = entities;
			this.respondBack = respondBack;
		}
		public List<ShardEntity> getEntities() {
			return entities;
		}
		/** @return TRUE if the leader must respond with replies and duty CommitState results */
		public boolean isRespondBack() {
			return respondBack;
		}
	}
	
	/**
	 * Response created by the Leader shard
	 * Specific replies for each entity in the passed batch.  
	 */
	public static class CommitBatchResponse extends CommitBatch {
		
		private static final long serialVersionUID = 5508717649512034997L;
		
		private final Set<Reply> replies;
		
		public CommitBatchResponse(final String requestId, final Set<Reply> replies) {
			super(requestId);
			this.replies = replies;
		}
		
		public Set<Reply> getReplies() {
			return replies;
		}
	}
}