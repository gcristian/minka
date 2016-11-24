/**
 * Copyright (c) 2011-2015 Zauber S.A. -- All rights reserved
 */

package io.tilt.minka.api;

import java.io.Serializable;
import java.util.Comparator;

import org.apache.commons.lang.Validate;

import io.tilt.minka.core.leader.distributor.Balancer.BalancerMetadata;

/**
 * A grouping factor for Duties with common and specific treatment requirements. </br> 
 * In logistics, pallets may group duties because of weight, destination, volume, color, product type, etc.
 * </br></br>
 * Playing with Pallets, you can achieve:
 * <li>     on-demand change of balancing strategies for duties,
 * <li>     ensure you have at least N duties assigned to each shard, this enables custom coordinators ! 
 * <li>     optimal exploitation of different finite phisical/virtual resources, to avoid biased duty workload calculation, 
 * <li>     specific storage type for every pallet, user custom (PartitionMaster) or use minka managment
 * </br></br>
 * 
 * Samples of Pallets are: 
 * <li>	CPU: threads, i/o, memory, 
 * <li>	Network bandwidth, 
 * <li>	Storage: space, throughput, 
 * <li>	Virtual: DB connections, files descriptors
 * 
 * Pallets like Duties, require a CRUD management by the host application.</br> 
 * As a grouping factor it's just a tag for Duties, they only contain attributes and behaviour configuration. 
 * Once created, duties can have the tag. Then operations on duties can be also handled by using the pallet tag.
 * They're ignored by the distributor when empty.
 * 
 * Pallets may also be prioritized at balancing when sharing the same finite resource.
 * For that matter the Delegate reckoning the pallet's resource and id: must report a biased capacity.   
 * 
 * They will be taken to PartitionDelegate first time they get involved into Shard's lifecycle when distribution 
 * dictates so, so the host app. can access their payload mostly, and be prepare to receive duties of the pallet.
 * 
 * <br><br> 
 * 
 * @author Cristian Gonzalez
 * @since Mar 30, 2016
 */
public interface Pallet<P extends Serializable> extends Entity<P> {

	public enum Storage {
		/**
		 * The host application must provide an instance of {@linkplain PartitionMaster}
		 * for minka to know them at leader election, before distribution.
		 * Also the provided instance will be called for CRUD operations, only at leader's shard. 
		 * So the client handles persistance, media transformation, database, etc.   
		 */
		CLIENT_DEFINED,
		/**
		 * Minka uses its own storage. Which spreads to JDBC, filesystem.
		 * With JDBC the host application can still have some control over data.  
		 */
		MINKA_MANAGEMENT
	}

	Storage getStorage();

	/** @return the Balancer strategy and metadata to use for the pallet */
	BalancerMetadata getMetadata();
	
	/** @return only informative and user usage: the finite resource (mostly physical) being used */
	default Resource getResource() {
		return null;
	}

	/** @return only to be used for balancers making use of a sorting previous to duty rebalance */
	default Comparator<Duty<P>> getPreSortComparator() {
		return null;
	}

	/** Represents the way that Duties inside a Pallet will exhaust a finite resource */
	public static class Resource implements Serializable {
		private static final long serialVersionUID = 1L;
		private final Link link;
		private final String id;
		private final String name;
		public Resource(Link link, String id, String name) {
			super();
			Validate.notNull(id);
			Validate.notNull(name);
			Validate.notNull(link);
			this.link = link;
			this.id = id;
			this.name = name;
		}
		public Link getLink() {
			return this.link;
		}
		public String getId() {
			return this.id;
		}
		public String getName() {
			return this.name;
		}
		@Override
		public boolean equals(Object obj) {
			if (obj == null) {
				return false;
			} else if (obj == this) { 
				return true;
			} else if (!(obj instanceof Pallet)) {
				return false;
			} else {
				return ((Resource)obj).getId().equals(getId());
			}
		}
		@Override
		public int hashCode() {
			return getId().hashCode();
		}
		
		public enum Link {
			/* explicit mathematical computation */
			CPU_CLOCK,
			/* parallelism for short-term tasks */
			CPU_THREADS,
			/* upload data */
			NETWORK_OUTBOUND,
			/* download data */
			NETWORK_INBOUND,
			/* take benefit of better position in the network */
			NETWORK_HIERARCHY,
			/* allocate transient data */
			MEMORY,
			/* disk size */
			STORAGE_SPACE,
			/* disk speed */
			STORAGE_THROUGHPUT
		}
	}

}