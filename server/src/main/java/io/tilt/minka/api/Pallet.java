package io.tilt.minka.api;

import java.util.Comparator;

import io.tilt.minka.core.leader.balancer.Balancer.BalancerMetadata;

/**
 * A grouping factor for Duties with common and specific treatment requirements. <br> 
 * In logistics, pallets may group duties because of weight, destination, volume, color, product type, etc.
 * <br><br>
 * Playing with Pallets, you can achieve:
 *      on-demand change of balancing strategies for duties,
 *      ensure you have at least N duties assigned to each shard, this enables custom coordinators ! 
 *      optimal exploitation of different finite phisical/virtual resources, to avoid biased duty workload calculation, 
 *      specific storage type for every pallet, user custom (PartitionMaster) or use minka managment
 * <br><br>
 * 
 * Samples of Pallets are: 
 * 	CPU: threads, i/o, memory, 
 * 	Network bandwidth, 
 * 	Storage: space, throughput, 
 * 	Virtual: DB connections, files descriptors
 * 
 * Pallets like Duties, require a CRUD management by the host application.<br> 
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
public interface Pallet extends Entity {


	/** @return the Balancer strategy and metadata to use for the pallet */
	BalancerMetadata getMetadata();
	
	/** @return only to be used for balancers making use of a sorting previous to duty rebalance */
	default Comparator<Duty> getPreSortComparator() {
		return null;
	}
	
	public static PalletBuilder builder(final String palletId) {
		return PalletBuilder.builder(palletId);
	}

	default int replicas() {
		throw new RuntimeException("not implemented");
	}

}