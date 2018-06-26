/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.tilt.minka.api;

import java.io.Serializable;

import org.apache.commons.lang.Validate;

public interface ResourcePallet extends Pallet {

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
