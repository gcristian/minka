package io.tilt.minka.core.leader.balancer;

import java.util.Comparator;

import io.tilt.minka.domain.EntityDateComparer;
import io.tilt.minka.domain.EntityHashComparer;
import io.tilt.minka.domain.EntityWeightComparer;
import io.tilt.minka.model.Duty;

public enum PreSort {
	/**
	 * Dispose duties with perfect mix between all workload values
	 * in order to avoid having two duties of the same workload together
	 * like: 1,2,3,1,2,3,1,2,3 = Good for migration reduction while balanced distrib.  
	 */
	SAW(null),
	/**
	 * Use hashing order
	 */
	HASH(new EntityHashComparer()),
	/**
	 * Use Creation date order, i.e. natural order.
	 * Use this to keep the migration of duties among shards: to a bare minimum.
	 * Duty workload weight is considered but natural order restricts the re-accomodation much more.
	 * Useful when the master list of duties has lot of changes in time, and low migration is required.
	 * Use this in case your Duties represent Tasks of a short lifecycle.
	 */
	DATE(new EntityDateComparer()),
	/**
	 * Use Workload order.
	 * Use this to maximize the clustering algorithm's effectiveness.
	 * In presence of frequent variation of workloads, duties will tend to migrate more. 
	 * Otherwise this's the most optimus strategy.
	 * Use this in case your Duties represent Data or Entities with a long lifecycle 
	 */
	WEIGHT(new EntityWeightComparer()),
	
	/** Use Pallet's custom comparator */
	CUSTOM(null),
	;
	
	private final Comparator<Duty> comp;
	PreSort(final Comparator<Duty> comp) {
		this.comp = comp;
	}
	public Comparator<Duty> getComparator() { 
		return this.comp;
	}
}