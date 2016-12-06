package io.tilt.minka.domain;

import java.io.IOException;
import java.net.URI;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.hk2.utilities.binding.AbstractBinder;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import io.tilt.minka.api.Endpoint;

public class WebServer {

    public static HttpServer startServer() throws IOException {
        final URI uri = URI.create("http://localhost:57480/minka/");
		final ResourceConfig conf = new ResourceConfig(Endpoint.class);
		/*(conf.register(new AbstractBinder() {
			@Override
			protected void configure() {
				// TODO Auto-generated method stub
				
			}
		});*/
		final HttpServer server = GrizzlyHttpServerFactory.createHttpServer(uri, 
				conf);
		server.start();
		return server;
    }

    public static void main(String[] args) throws Exception {
        
        //final HttpServer server = startServer();
        new DatasetSampler().startClientApp();
        Thread.sleep(60000*10);
        //server.shutdown();
        
    }
}


