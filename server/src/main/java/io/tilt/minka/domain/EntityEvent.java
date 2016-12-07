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

	/* user creates a duty from MinkaClient */
	CREATE(true),
	/* user prompts to delete as a kill state */
	REMOVE(true),
	/* user updates something related to the duty that leader must notify the shard */
	UPDATE(true),
	/* unrelated to the entity, just a message to it's delegate */
	TRANSFER(false),

	/* leader assigns to a Shard */
	ATTACH(false),
	/* leader takes off the duty from the shard for any reason may be */
	DETACH(false),
	/* the duty has finalized */
	FINALIZED(true);

	boolean crud;

	EntityEvent(boolean crud) {
		this.crud = crud;
	}

	public boolean isCrud() {
		return crud;
	}

	public boolean is(EntityEvent pe) {
		return this == pe;
	}

	@Override
	public String toString() {
		return this.name().substring(0, 1);
	}
}