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
package io.tilt.minka.api.inspect;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.CacheControl;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.spi.LoggingEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.wordnik.swagger.annotations.Api;

import io.tilt.minka.api.Config;
import io.tilt.minka.core.leader.PartitionScheme;
import io.tilt.minka.core.task.Scheduler;

@Api("Minka Endpoint API")
@Path("admin")

@Singleton
@Component
public class AdminEndpoint {

	@Autowired
	private PartitionScheme table;
	@Autowired
	private SchemeViews views;
	@Autowired
	private Scheduler scheduler;
	@Autowired
	private Config config;

	@Inject
	public AdminEndpoint(@Named("partitionTable") PartitionScheme table) {
		this.table = table;
	}
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
	@Path("/config")
	@Produces(MediaType.APPLICATION_JSON)
	public Response config() throws Exception {
		return Response.accepted(config.toJson()).build();
	}
	
	@GET
	@Path("/distro")
	@Produces(MediaType.APPLICATION_JSON)
	public Response status() throws JsonProcessingException {
		return Response.accepted(views.distributionToJson(table)).build();
	}

	@GET
	@Path("/pallets")
	@Produces(MediaType.APPLICATION_JSON)
	public Response pallets() throws JsonProcessingException {
		return Response.accepted(views.palletsToJson(table)).build();
	}
	                    
	@GET
	@Path("/shards")
	@Produces(MediaType.APPLICATION_JSON)
	public Response shards() throws JsonProcessingException {
		return Response.accepted(views.shardsToJson(table)).build();
	}

	@GET
	@Path("/duties")
	@Produces(MediaType.APPLICATION_JSON)
	public Response duties() throws JsonProcessingException {
		return Response.accepted(views.dutiesToJson(table)).build();
	}

	@GET
	@Path("/entities")
	@Produces(MediaType.APPLICATION_JSON)
	public Response entities() throws JsonProcessingException {
		return Response.accepted(views.entitiesToJson(table)).build();
	}

	@GET
	@Path("/schedule")
	@Produces(MediaType.APPLICATION_JSON)
	public Response schedule() throws JsonProcessingException {
		return Response.accepted(views.scheduleToJson(scheduler)).build();
	}

	@GET
	@Path("/plans")
	@Produces(MediaType.APPLICATION_JSON)
	public Response plans() throws JsonProcessingException {
		final Map<String, Object> map = new HashMap<>(2);
		map.put("plans", table.getHistory().size());
		map.put("history", table.getHistory());
        return Response.accepted(views.elementToJson(map)).build();
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
