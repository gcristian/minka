package io.tilt.minka.core.leader.data;

public enum ConsistencyLevel {
	
	/** fire operation without waiting for results  */
	FIRE_AND_FORGET,
	/** execute and wait at least 1 follower shard to commit */
	ONE_SHARD,	
	/** execute and wait all follower shards to commit */
	ALL_SHARDS
	
}