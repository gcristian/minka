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
package io.tilt.minka.shard;

import java.io.Serializable;

import org.joda.time.DateTime;

import io.tilt.minka.core.follower.FollowerBootstrap;

/**
 * Identity of a shard (followers and leader alike)
 * Created by the {@linkplain FollowerBootstrap}
 * 
 * @author Cristian Gonzalez
 * @since Dec 3, 2015
 *
 */
public interface ShardIdentifier extends Serializable {

	/** to be used internally to identify follower shards */
	String getId();

	/** to be used by users as a secondary reference */
	String getTag();

	DateTime getCreation();

	/* receive it after server initialization */
	void setWebHostPort(String hostport);

	String getWebHostPort();

}
