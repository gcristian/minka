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

	/* user creates a duty from Client */
	CREATE('c', true, "Creating"),
	/* user prompts to delete as a kill state */
	REMOVE('r', true, "Removing"),
	/* user updates something related to the duty that leader must notify the shard */
	UPDATE('u', true, "Updating"),
	/* unrelated to the entity, just a message to it's delegate */
	TRANSFER('t', false, "Transferring" ),

	/* leader assigns to a Shard */
	ATTACH('a', false, "Attaching"),
	/* leader takes off the duty from the shard for any reason may be */
	DETACH('d', false, "Dettaching"),
	
	;

	private final boolean crud;
	private final char code;
	private final String verb;

	EntityEvent(final char code, final boolean crud, final String verb) {
	    this.code = code;
		this.crud = crud;
		this.verb = verb;
	}

	public boolean isCrud() {
		return crud;
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