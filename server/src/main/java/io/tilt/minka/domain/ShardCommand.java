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

import org.apache.commons.lang.Validate;
import org.joda.time.DateTime;

import io.tilt.minka.api.EntityPayload;
import io.tilt.minka.api.MinkaClient.Command;

/**
 * An operation to be executed at the cluster master
 * @author Cristian Gonzalez
 * @since Nov 8, 2015
 *
 */
public class ShardCommand implements EntityPayload {

	private static final long serialVersionUID = 5911601577093197314L;

	private Command command;
	private DateTime creation;
	private NetworkShardID shardId;

	public ShardCommand(final Command command, final NetworkShardID shardId) {
		super();
		this.command = command;
		Validate.isTrue(
				command == Command.FOLLOWER_DEACTIVATE || command == Command.FOLLOWER_ACTIVATE
						|| command == Command.FOLLOWER_DECOMISSION || command == Command.FOLLOWER_HOARD,
				"this type is inalid for a host");
		this.shardId = shardId;
	}

	public NetworkShardID getShardId() {
		return this.shardId;
	}

	public Command getOperation() {
		return this.command;
	}

	public void setOperation(Command operation) {
		this.command = operation;
	}

}
