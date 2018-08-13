package io.tilt.minka.core.monitor;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.spi.Filter;
import org.apache.log4j.spi.LoggingEvent;


public class OnDemandAppender implements org.apache.log4j.Appender {

	private final UniqueAppender appender = UniqueAppender.getInstance();
	
	public static class UniqueAppender {
		public static UniqueAppender a = new UniqueAppender();
		private BlockingQueue<LoggingEvent> queue = new ArrayBlockingQueue<LoggingEvent>(10);
		private boolean inUse;
		private UniqueAppender() {
		}
		public synchronized static UniqueAppender getInstance() {
			return a;
		}
		public boolean isInUse() {
			return inUse;
		}
		public synchronized void setInUse(boolean inUse) {
			this.inUse = inUse;
		}
		public BlockingQueue<LoggingEvent> getQueue() {
			return this.queue;
		}
		public void offer(final LoggingEvent log) {
			if (inUse) {
				if (!queue.offer(log)) {
					queue.poll();
					queue.offer(log);
				}
			}
		}
	}

	@Override
	public void addFilter(Filter newFilter) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Filter getFilter() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void clearFilters() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
		appender.setInUse(false);
	}

	@Override
	public void doAppend(LoggingEvent event) {
		appender.offer(event);
	}

	@Override
	public String getName() {
		// TODO Auto-generated method stub
		return "OnDemandAppender";
	}

	@Override
	public void setErrorHandler(org.apache.log4j.spi.ErrorHandler errorHandler) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public org.apache.log4j.spi.ErrorHandler getErrorHandler() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setLayout(org.apache.log4j.Layout layout) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public org.apache.log4j.Layout getLayout() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setName(String name) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean requiresLayout() {
		// TODO Auto-generated method stub
		return false;
	}


}
