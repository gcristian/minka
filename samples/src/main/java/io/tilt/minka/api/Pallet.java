/**
 * Copyright (c) 2011-2015 Zauber S.A. -- All rights reserved
 */

package io.tilt.minka.api;

import java.io.Serializable;

import io.tilt.minka.core.leader.distributor.Balancer.BalanceStrategy;
import io.tilt.minka.core.leader.distributor.FairWorkloadBalancer.PreSortType;
import io.tilt.minka.core.leader.distributor.SpillOverBalancer.MaxValueUsage;

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

	/**
	 * For every distribution ocasion, using the BalanceStrategy for duty workload: pallets are first 
	 * balanced in as many fragments as shards online.
	 *   
	 * @return
	 */
	BalanceStrategy getBalanceStrategy();

	/*
	 * TODO refactor all this into a builder for fair and spill balancer config
	 */
	default PreSortType getBalancerFairLoadPresort() {
		return PreSortType.DATE;
	}

	default MaxValueUsage getSpillBalancerStrategy() {
		return MaxValueUsage.SIZE;
	}

	default int getBalancerSpillOverMaxValue() {
		return 1;
	}

	default int getBalancerEvenSizeMaxDutiesDeltaBetweenShards() {
		return 3;
	}

	/**
	 * If applies, after balancing pallet's duties throughout shards,
	 * pallets are re-located to different shards according different PalletBalanceStrategy
	 * using the sum of their duties ( duty's workload for FAIR_LOAD and SPILL_OVER ) 
	 */
	public enum PalletBalanceStrategy {
		/* like their BalanceStrategy counter part */
		EVEN_SIZE,
		/* like their BalanceStrategy counter part */
		FAIR_LOAD,
		/* like their BalanceStrategy counter part */
		SPILL_OVER,
		/* others to be designed */
		PRODUCT_TYPE,
	}

	default boolean applyPalletBalancing() {
		return false;
	}

	default PalletBalanceStrategy getPalletBalanceStrategy() {
		return PalletBalanceStrategy.EVEN_SIZE;
	}

}