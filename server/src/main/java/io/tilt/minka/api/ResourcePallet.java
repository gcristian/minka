/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.api;

import java.io.Serializable;

import org.apache.commons.lang.Validate;

import io.tilt.minka.api.ResourcePallet.Resource;

public interface ResourcePallet extends Pallet<Resource> {

	/** @return only informative and user usage: the finite resource (mostly physical) being used */
	default Resource getResource() {
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
