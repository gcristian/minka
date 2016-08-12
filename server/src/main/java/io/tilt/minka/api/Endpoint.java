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

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.wordnik.swagger.annotations.Api;

import io.tilt.minka.domain.NetworkShardIDImpl;
import io.tilt.minka.domain.ShardCommand;

@Api("Minka Endpoint API")
@Path("/minka/{service}")
@Produces(MediaType.APPLICATION_JSON)
@Singleton
public class Endpoint {

		private final PartitionService partitionService;

		@Inject
		public Endpoint(@Named("partitionService") final PartitionService partitionService) {
			this.partitionService = partitionService;
		}

		@POST
		@Path("/{type}")
		@Consumes(MediaType.APPLICATION_JSON)
		public Response clusterCommand(@PathParam("service") String service, @PathParam("command") ShardCommand command) {

			if (partitionService.execute(service, command)) {
				return Response.accepted().build();
			} else {
				return Response.serverError().build();
			}
		}

		@POST
		@Path("/{shardId}/{command}")
		@Consumes(MediaType.APPLICATION_JSON)
		/**
		 * 
		 * @param service   "mulita" for instance 
		 * @param shardId   "198.82.123.11" the follower ID
		 * @param type      
		 * @return
		 */
		public Response shardCommand(@PathParam("service") String service,
				@PathParam("shardId") NetworkShardIDImpl shardId, @PathParam("command") ShardCommand command) {

			if (partitionService.execute(service, command)) {
				return Response.accepted().build();
			} else {
				return Response.serverError().build();
			}
		}

}
