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
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.wordnik.swagger.annotations.Api;

import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.core.leader.Status;
import io.tilt.minka.core.leader.distributor.Plan;

@Api("Minka Endpoint API")
@Path("minka/admin")

@Singleton
@Component
public class AdminEndpoint {

	@Autowired
	private  PartitionTable table;

	@Autowired
	private  Config config;

	@Inject
	public AdminEndpoint(@Named("partitionTable") PartitionTable table) {
		this.table = table;
	}

	@GET
	@Path("/config")
	@Produces(MediaType.APPLICATION_JSON)
	public Response config() throws Exception {
		return Response.accepted(config.toJson()).build();
	}
	
	@GET
	@Path("/distro")
	@Produces(MediaType.APPLICATION_JSON)
	public Response status() throws JsonProcessingException {
		return Response.accepted(Status.toJson(table)).build();
	}

	@GET
	@Path("/pallets")
	@Produces(MediaType.APPLICATION_JSON)
	public Response pallets() throws JsonProcessingException {
		return Response.accepted(Status.palletsToJson(table)).build();
	}
	                    
	@GET
	@Path("/shards")
	@Produces(MediaType.APPLICATION_JSON)
	public Response shards() throws JsonProcessingException {
		return Response.accepted(Status.Shards.toJson(table)).build();
	}

	@GET
	@Path("/duties")
	@Produces(MediaType.APPLICATION_JSON)
	public Response duties() throws JsonProcessingException {
		return Response.accepted(Status.dutiesToJson(table)).build();
	}

	@GET
	@Path("/plans")
	@Produces(MediaType.APPLICATION_JSON)
	public Response plans() throws JsonProcessingException {
		final List<Plan> history = table.getHistory();
		final Map<String, Object> map = new HashMap<>(2);
		map.put("plans", history.size());
		map.put("history", history);
        return Response.accepted(Status.elementToJson(map)).build();
	}


}
