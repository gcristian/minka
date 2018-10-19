/*
 * Licensed to the Apache Sof-tware Foundation (ASF) under one or more
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

import javax.inject.Singleton;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.wordnik.swagger.annotations.Api;

import io.tilt.minka.core.leader.balancer.Balancer;
import io.tilt.minka.model.Duty;
import io.tilt.minka.model.Pallet;

@Api("Minka CRUD API facility")
@Path("crud")

@Singleton
@Component
public class CRUDEndpoint {

	private final Logger logger = LoggerFactory.getLogger(getClass());

	@Autowired	private Client client;

	private final static String PATH_CREATE_PALLET = "/pallet/{id}"; 
	@PUT
	@Path(PATH_CREATE_PALLET)
	@Produces(MediaType.APPLICATION_JSON)
	public Response createPallet(
			@PathParam("id") final String palletId,
			@QueryParam("strategy") String strategy) throws JsonProcessingException {
		try {		
			Balancer.BalancerMetadata bm = Balancer.Strategy.EVEN_SIZE.getBalancerMetadata();
			if (strategy!=null) {
				bm = Balancer.Strategy.valueOf(strategy).getBalancerMetadata();
			}
			final Pallet p = Pallet.builder(palletId).with(bm).build();
			client.add(p);
			return Response.accepted("").build();
		} catch (Exception e) {
			logger.error("while " + PATH_CREATE_PALLET, e);
			return Response.serverError().build();
		}
	}

	final static String PATH_SHARD_CAPACITY = "/capacity/pallet/{id}/{capacity}";
	@PUT
	@Path(PATH_SHARD_CAPACITY)
	@Produces(MediaType.APPLICATION_JSON)
	public Response shardCapacity(
			@PathParam("id") final String palletId,
			@PathParam("capacity") final String capacity) throws JsonProcessingException {
		
		try {		
			double cap = 9999;
			if (capacity!=null) {
				cap = Long.parseLong(capacity);
			}
			final Pallet p = Pallet.builder(palletId).build();
			client.getEventMapper().setCapacity(p, cap);
			return Response.accepted().build();
		} catch (Exception e) {
			logger.error("while " + PATH_SHARD_CAPACITY, e);
			return Response.serverError().build();
		}
	}

	// this's just a facility for testing endpoints
	// in no way represents minka's concepts whatsoever
	public static enum EndpointMode {
		/**
		 * fire and forget: client doesnt get leader's response
		 * unless lives within the follower JVM 
		 */
		ff,
		/**
		 * non blocking: client gets a future with leader's response
		 * no matter where it lives, 
		 * sucessful replies: has futures for each duty's CommitState result
		 */
		nb,
		/** 
		 * blocking: client blocks until gets leader's response
		 * successfull replies: has futures for each duty CommitState result
		 * for which client will also block the current thread. 
		 */
		b
	}
	
	final static String PATH_CREATE_DUTY = "/duty/{palletid}/{id}";
	@PUT
	@Path(PATH_CREATE_DUTY)
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Response createDuty(
			@PathParam("palletid") final String palletId,
			@PathParam("id") final String dutyId,
			@QueryParam("weight") final String weight,
			@QueryParam("mode") final EndpointMode mode) throws JsonProcessingException {
		
		try {
			long w = 1;
			if (weight!=null) {
				w = Long.parseLong(weight);
			}
			final Duty d = Duty.builder(dutyId, palletId).with(w).build();

			final Reply r;
			if (mode==EndpointMode.ff) {
				r = client.fireAndForget().add(d);
			} else if (mode==EndpointMode.nb){
				client.nonBlocking().add(d);
				return Response.accepted()
						.status(204)
						.build();
			} else if (mode==EndpointMode.b){
				r = client.add(d);
				if (r.isSuccess()) {
					r.getState().get();
				}
			} else {
				return Response.status(400).build();	
			}
			return Response.accepted(r)
					.status(r.getValue().getHttpCode())
					.build();
		} catch (Exception e) {
			logger.error("while " + PATH_CREATE_DUTY, e);
			return Response.serverError().build();
		}
	}
	
	final static String PATH_DELETE_DUTY = "/duty/{palletid}/{id}";
	@DELETE
	@Path(PATH_DELETE_DUTY)
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteDuty(
			@PathParam("palletid") final String palletId,
			@PathParam("id") final String dutyId,
			@QueryParam("mode") final EndpointMode mode) throws JsonProcessingException {
		try {
			final Duty d = Duty.builder(dutyId, palletId).build();
			

			final Reply r;
			if (mode==EndpointMode.ff) {
				// mode fire and forget: client doesnt get leader's response 
				// unless lives within the follower JVM
				r = client.fireAndForget().remove(d);
			} else if (mode==EndpointMode.nb){
				// mode non blocking: client gets a future with leader's response
				// no matter where it lives, sucessful replies: has futures for each duty's CommitState result
				client.nonBlocking().remove(d);
				return Response.accepted()
						.status(204)
						.build();
			} else if (mode==EndpointMode.b){
				// mode blocking: client blocks until gets leader's response 
				// successfull replies: has futures for each duty CommitState result
				// for which client will also block the current thread.
				r = client.remove(d);
				if (r.isSuccess()) {
					r.getState().get();
				}
			} else {
				return Response.status(400).build();	
			}
			return Response.accepted(r)
					.status(r.getValue().getHttpCode())
					.build();		
		} catch (Exception e) {
			logger.error("while " + PATH_DELETE_DUTY, e);
			return Response.serverError().build();
		}
	}

}
