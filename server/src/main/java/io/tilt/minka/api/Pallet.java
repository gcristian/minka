/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.api;

import io.tilt.minka.business.leader.distributor.Balancer.BalanceStrategy;

/**
 * A grouping factor for Duties with common and specific treatment requirements. 
 * In logistics, pallets may group duties because of weight, destination, volume, color, product type, etc.
 * </br></br>
 * Playing with Pallets, you can achieve several features like:
 * <li>     on-demand change of balancing strategies for duties,
 * <li>     ensure you have at least N duties assigned to each shard, this enables custom coordinators ! 
 * <li>     optimal exploitation of different finite resources, to avoid mixing them when calculating duty workload, 
 *          like physical (cpu,i/o, mem, HD space) or virtual (connections, files, etc)
 * <li>     specific storage type for every pallet, user custom (PartitionMaster) or use minka managment
 *          <br><br>
 *          
 * Pallets like Duties, require a CRUD management by the host application.</br> 
 * As a grouping factor it's just a tag for Duties, they only contain attributes and behaviour configuration. 
 * Once created, duties can have the tag. Then operations on duties can be also handled by using the pallet tag.
 * They're ignored by the distributor when empty.
 * 
 * <br><br> 
 * 
 * @author Cristian Gonzalez
 * @since Mar 30, 2016
 */
public interface Pallet<P> extends Entity <P> {

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
    
    /**
     * For every distribution ocasion, using the BalanceStrategy for duty workload: pallets are first 
     * balanced in as many fragments as shards online.
     * Then pallets are located to different shards according different PalletBalanceStrategy
     *   
     * @return
     */
    BalanceStrategy getBalanceStrategy();
   
}