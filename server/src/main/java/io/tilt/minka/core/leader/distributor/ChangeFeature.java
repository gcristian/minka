package io.tilt.minka.core.leader.distributor;

/** reasons for {@linkplain ChangePlan} will be built */
public enum ChangeFeature {
	/** fallen shard saved duties */
	RESTORE_DANGLING,
	/** existing shard dropped duties */
	RESTORE_MISSING,
	/** restoring previous plan pending never confirmed duties */
	RESTORE_UNFINISHED,
	/** a change in the shards registered in the cluster*/
	CLUSTER_EXPAND,
	CLUSTER_SHRINK,
	/** limited dirty state with remaining changes */
	LIMITED_PROMOTION,
	/** client triggered actions */
	COMMIT_REQUEST,
	/** follower's clearance expired beyond leader ack */
	CLEARANCE_EXPIRED,
	/** follower proved heart didnt beat for long enough */
	HEARTATTACK,
	/** new replication triggered a rebalance */
	REPLICATION_EVENTS,
	/** a redistribution caused by a balancer with no environment change */
	REBALANCE,
}