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

import io.tilt.minka.api.Pallet.Storage;

public class ConfigValidator {

	/* TODO este metodo va a crecer mucho y requiere un monton de calculo */
	public void validate(NewConfig config, PartitionDelegate delegate) {

		if (config.getBroker().getHostPort().indexOf(":") < 1) {
			throw new IllegalArgumentException(
					"Broker host string (ip:port) invalid format = " + config.getBroker().getHostPort());
		}
		checkPartitionMaster(config, delegate);
	}

	private void checkPartitionMaster(NewConfig config, PartitionDelegate delegate) {
		if (delegate instanceof PartitionMaster && config.getConsistency().getDutyStorage() == Storage.MINKA_MANAGEMENT) {
			throw new RuntimeException("You must provide a PartitionDelegate instead of PartitionMaster"
					+ " while having configuration parameter Storage = MINKA_MANAGEMENT");
		} else if (!(delegate instanceof PartitionMaster) && config.getConsistency().getDutyStorage() == Storage.CLIENT_DEFINED) {
			throw new RuntimeException("You must provide a PartitionMaster instead of PartitionDelegate"
					+ " while having configuration parameter Storage = CLIENT_DEFINED");
		}
	}

}
