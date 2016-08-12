/**
 * Copyright (c) 2011-2015 Zauber S.A. -- All rights reserved
 */

package io.tilt.minka.api;

/**
 * Options to dinamically customize sharding of duties 
 */
public enum DutyAttribute {

	/**
	 * They will only be sent when there's  
	 */
	QUEUED, 
	/** 
	 * Duties that once assigned and started: cannot 
	 * be migrating for balancing purposes.
	 * They somewhat rely on local resources or cannot 
	 * store State to continue working from a savepoint after re-assigned 
	 */
	DISABLE_MIGRATION, 
	/**
	 * Duties that dont need to be removed, once assigned 
	 * and reported, next time is absent will be interpreted 
	 * as automatically ended and will not be re-assigned
	 */
	ENABLE_AUTO_FINALIZATION,;
}
