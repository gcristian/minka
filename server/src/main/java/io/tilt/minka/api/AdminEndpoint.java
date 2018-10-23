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

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

import io.tilt.minka.core.leader.balancer.Balancer;
import io.tilt.minka.core.leader.data.Scheme;
import io.tilt.minka.core.monitor.CrossJSONBuilder;
import io.tilt.minka.core.monitor.DistroJSONBuilder;
import io.tilt.minka.core.monitor.FollowerJSONBuilder;
import io.tilt.minka.core.monitor.OnDemandAppender;
import io.tilt.minka.core.monitor.SchemeJSONBuilder;

@Api("Minka Endpoint API")
@Path("admin")

@Singleton
@Component
public class AdminEndpoint {

	@Autowired	private Scheme scheme;
	@Autowired	private CrossJSONBuilder global;
	@Autowired	private FollowerJSONBuilder follower;
	@Autowired	private DistroJSONBuilder distro;
	@Autowired	private SchemeJSONBuilder schemeJSONBuilder;
	@Autowired	private Config config;
	@Autowired	private Client client;

	/*
	
	@GET
    @Path("view")
    public Viewable index(@Context HttpServletRequest request) {
        request.setAttribute("obj", new String("IT Works"));
        System.out.println("/INDEXed");
        return new Viewable("/index.jsp", null);
    }

*/
	
	@GET
	@Path("/help")
	@Produces(MediaType.APPLICATION_JSON)
	public Response help() throws Exception {
		final Map<String, String> ret = new LinkedHashMap<>();
		ret.put("/config", "show initial settings");
		ret.put("/distro", "show duty distribution status");
		ret.put("/distro/run", "run distribution on demand");
		ret.put("/broker", "show event broker stats");
		ret.put("/pallets", "show pallets in scheme");
		ret.put("/beats", "show heartbeats from follower");
		ret.put("/shards", "show cluster members");
		ret.put("/scheme", "show available and stage duties");
		ret.put("/partition", "show duty partition on current shard");
		ret.put("/plans", "show distribution change plans");
		ret.put("/log/text", "capture logging on demand");
		ret.put(PATH_CREATE_PALLET, "create pallet");
		ret.put(PATH_CREATE_DUTY, "create duty");
		ret.put(PATH_DELETE_DUTY, "delete duty");
		ret.put(PATH_SHARD_CAPACITY, "set shard capacity");
		return Response.accepted(ret).build();
	}
	
	private final Logger logger = LoggerFactory.getLogger(getClass());

	@GET
	@Path("/config")
	@Produces(MediaType.APPLICATION_JSON)
	public Response config() throws Exception {
		try {
			return Response.accepted(config.toJson()).build();	
		} catch (Exception e) {
			logger.error("while /config", e);
			return Response.serverError().build();
		}
	}
	
	@GET
	@ApiOperation("shows distribution strategy")
	@Path("/distro")
	@Produces(MediaType.APPLICATION_JSON)
	public Response status() throws JsonProcessingException {
		try {
			return Response.accepted(distro.distributionToJson()).build();
		} catch (Exception e) {
			logger.error("while /distro", e);
			return Response.serverError().build();
		}
	}

	@GET
	@Path("/broker")
	@Produces(MediaType.APPLICATION_JSON)
	public Response broker() throws JsonProcessingException {
		try {
			return Response.accepted(global.brokerToJson()).build();	
		} catch (Exception e) {
			logger.error("while /broker", e);
			return Response.serverError().build();
		}
	}

	@GET
	@Path("/pallets")
	@Produces(MediaType.APPLICATION_JSON)
	public Response pallets() throws JsonProcessingException {
		try {
			return Response.accepted(distro.palletsToJson()).build();	
		} catch (Exception e) {
			logger.error("while /pallets", e);
			return Response.serverError().build();
		}
	}
	                    
	@GET
	@Path("/shards")
	@Produces(MediaType.APPLICATION_JSON)
	public Response shards() throws JsonProcessingException {
		try {
			return Response.accepted(distro.shardsToJson()).build();
		} catch (Exception e) {
			logger.error("while /shards", e);
			return Response.serverError().build();
		}
	}

	@GET
	@Path("/scheme")
	@Produces(MediaType.APPLICATION_JSON)
	public Response scheme(@QueryParam("detail") final boolean detail) throws JsonProcessingException {
		try {
			return Response.accepted(schemeJSONBuilder.schemeToJson(detail)).build();
		} catch (Exception e) {
			logger.error("while /scheme", e);
			return Response.serverError().build();
		}
	}

	@GET
	@Path("/partition")
	@Produces(MediaType.APPLICATION_JSON)
	/** @return the follower's sharded partition entities */
	public Response partition(@QueryParam("detail") final boolean detail) throws JsonProcessingException {
		try {
			return Response.accepted(follower.partitionToJson(detail)).build();
		} catch (Exception e) {
			logger.error("while /partition", e);
			return Response.serverError().build();
		}
	}

	@GET
	@Path("/scheduler")
	@Produces(MediaType.APPLICATION_JSON)
	public Response schedule(@QueryParam("detail") final boolean detail) throws JsonProcessingException {
		try {
			return Response.accepted(global.scheduleToJson(detail)).build();
		} catch (Exception e) {
			logger.error("while /scheduler", e);
			return Response.serverError().build();
		}
	}
	
	@GET
	@Path("/beats")
	@Produces(MediaType.APPLICATION_JSON)
	public Response beats() throws JsonProcessingException {
		try {
	        return Response.accepted(follower.beatsToJson()).build();
		} catch (Exception e) {
			logger.error("while /beats", e);
			return Response.serverError().build();
		}
	}
	@GET
	@Path("/plans")
	@Produces(MediaType.APPLICATION_JSON)
	public Response plans(@QueryParam("detail") final boolean detail) throws JsonProcessingException {
		try {
	        return Response.accepted(distro.plansToJson(detail)).build();
		} catch (Exception e) {
			logger.error("while /plans", e);
			return Response.serverError().build();
		}
	}
	
	// =========================================================================================================
	
	@POST
	@Path("/distro/run")
	@Produces(MediaType.APPLICATION_JSON)
	public Response runDistro() throws JsonProcessingException {
		try {
			scheme.getCommitedState().stealthChange(true);
			return Response.accepted().build();
		} catch (Exception e) {
			logger.error("while /distro/run", e);
			return Response.serverError().build();
		}
	}

	private final static String PATH_CREATE_PALLET = "/crud/pallet/{id}"; 
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

	private final static String PATH_SHARD_CAPACITY = "/capacity/pallet/{id}/{capacity}";
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

	private final static String PATH_CREATE_DUTY = "/crud/duty/{palletid}/{id}";
	@PUT
	@Path(PATH_CREATE_DUTY)
	@Produces(MediaType.APPLICATION_JSON)
	public Response createDuty(
			@PathParam("palletid") final String palletId,
			@PathParam("id") final String dutyId,
			@QueryParam("weight") final String weight,
			@QueryParam("cl") final int consistencyLevel) throws JsonProcessingException {
		
		try {
			long w = 1;
			if (weight!=null) {
				w = Long.parseLong(weight);
			}
			final Duty d = Duty.builder(dutyId, palletId).with(w).build();
			final Future<Reply> f = client.add(d);
			final Reply r = f.get();
			// 0 = fire and forget, 
			// 1 = leader ack, 
			// 2 = first follower ack, 
			// 3 = all followers ack.
			if (r.isSuccess() && consistencyLevel>0) {
				r.getState().get();
			}
			return Response.accepted(r)
					.status(r.getValue().getHttpCode())
					.build();
		} catch (Exception e) {
			logger.error("while " + PATH_CREATE_DUTY, e);
			return Response.serverError().build();
		}
	}

	private final static String PATH_DELETE_DUTY = "/crud/duty/{palletid}/{id}";
	@DELETE
	@Path(PATH_DELETE_DUTY)
	@Produces(MediaType.APPLICATION_JSON)
	public Response deleteDuty(
			@PathParam("palletid") final String palletId,
			@PathParam("id") final String dutyId,
			@QueryParam("cl") final int consistencyLevel) throws JsonProcessingException {
		try {
			final Duty d = Duty.builder(dutyId, palletId).with(1).build();
			final Reply r = client.remove(d).get();
			if (r.isSuccess() && consistencyLevel>0) {
				r.getState().get();
			}
			return Response.accepted(r)
					.status(r.getValue().getHttpCode())
					.build();
		} catch (Exception e) {
			logger.error("while " + PATH_DELETE_DUTY, e);
			return Response.serverError().build();
		}
	}

	
	public enum Format {
		TEXT {
			@Override
			String format(LoggingEvent e) {
				return new StringBuilder()
						//.append(ofEpochMilli(e.getTimeStamp()).atZone(systemDefault()).toString()).append(' ')
						//.append(e.get)
						//.append(e.getLevel().toString()).append(' ')
						.append(e.getRenderedMessage()).append('\n')
						.toString();
						/*
					.append(e.getTimeStamp()).append(' ')
					.append(e.getThreadName()).append(' ')
					.append(e.getLevel()).append(' ')
					.append(e.getMessage().toString())
					.append('\n')
					.toString();*/
			}
		},
		HTML {
			@Override
			String format(LoggingEvent e) {
				return new StringBuilder()
						.append(e.getRenderedMessage())
						.append("</br>")
						.toString();

				/*
				return new StringBuilder()
						.append(e.getTimeStamp()).append(' ')
						.append(e.getThreadName()).append(' ')
						.append(e.getLevel()).append(' ')
						.append(e.getMessage().toString())
						.append("</br>")
						.toString();
						*/
			}
		};
		abstract String format(final LoggingEvent e);
	}
	
    @GET
    @Path("/log/{type}")
	public Response log(@PathParam("type") final String type) {
    	final OnDemandAppender.UniqueAppender log = OnDemandAppender.UniqueAppender.getInstance();
    	try {
	        final StreamingOutput out = new StreamingOutput() {
				@Override
				public void write(OutputStream out) throws IOException, WebApplicationException {
				    try {
				    	log.setInUse(true);
				    	out.write(StringUtils.repeat(' ', 8092).getBytes(UTF_8));
				    	out.write("\n".getBytes(UTF_8));
				    	out.flush();
				    	try {
				    		int maxEmptyLoops = 60 * 5;
					    	while (maxEmptyLoops>0) {
					    		log.setInUse(true);
					    		final LoggingEvent msg = log.getQueue().poll(1l, TimeUnit.SECONDS);
					    		if (msg!=null) {
					    			out.write(Format.valueOf(type.toUpperCase()).format(msg).getBytes(UTF_8));
					    			out.flush();
					    		} else {
					    			maxEmptyLoops--;
					    		}
					    	}
						} catch (InterruptedException e) {
						}
				    } finally {
				    }
				}
			};
			
			return Response.ok(out)
					//.cacheControl(CacheControl.valueOf("no-store"))
					//.header("Content-Length", "10")
					.build();
			
    	} finally {
    		log.setInUse(false);
    	}
    }


}
