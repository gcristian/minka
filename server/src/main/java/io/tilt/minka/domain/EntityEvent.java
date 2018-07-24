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
package io.tilt.minka.domain;

/**
 * A partitioning event onto a sharding duty.
 * Later this value will hop from one to another as a cycle or recycle
 * depending on the State value. 
 */
public enum EntityEvent {

	/** user creates a duty from Client */
	CREATE('c', "Creating", Type.CRUD),
	/** user prompts to delete as a kill state */
	REMOVE('r', "Removing", Type.CRUD),
	/** user updates something related to the duty that leader must notify the shard */
	UPDATE('u', "Updating", Type.CRUD),
	/** unrelated to the entity, just a message to it's delegate */
	TRANSFER('t', "Transferring", Type.NONE),

	/** leader assigns to a Shard */
	ATTACH('a', "Attaching", Type.ALLOCATION),
	/** leader takes off the duty from the shard for any reason may be */
	DETACH('d', "Dettaching", Type.ALLOCATION),
	
	/** leader backs up it's follower's duties to other followers as a master backup */
	STOCK('s', "Stocking", Type.REPLICA),
	DROP('p', "Dropping", Type.REPLICA)
	
	;

	private final char code;
	private final String verb;
	
	private final Type type;

	EntityEvent(final char code, final String verb, final Type type) {
	    this.code = code;
		this.verb = verb;
		this.type = type;
	}
	
	public Type getType() {
		return type;
	}

	public enum Type {
		/** type creation/read/update/delete */
		CRUD,
		/** type of a distribution action: attach (capture) or detach (release) */
		ALLOCATION, 
		/** type of a master backup action: stocking or dropping */
		REPLICA, 
		// none
		NONE
	}

	public EntityEvent getRootCause() {
		switch (this) {
		case ATTACH:
			return CREATE;
		case DETACH:
			return REMOVE;
		default:
			return null;
		}
	}

	public boolean is(EntityEvent pe) {
		return this == pe;
	}
	
	public String toVerb() {
		return verb;
	}

	public char getCode() {
	    return this.code;
	}
	
	public EntityEvent fromCode(final char code) {
		for (EntityEvent ee : EntityEvent.values()) {
			if (ee.getCode() == code) {
				return ee;
			}
		}
		throw new IllegalArgumentException("entity event code: " + code + " doesnt exist");
	}
	
/*	
    @Override
	public String toString() {
		return this.name().substring(0, 1);
	}
*/
}