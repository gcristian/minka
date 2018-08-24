package io.tilt.minka.utils;

import java.text.MessageFormat;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** 
 * Acts as a dummy decorator for logging.
 * a facility to let the Client Application to configure Inductor logging events to its own need, 
 * allowing a backdoor to tail recent volatile events, view only when it's being watched.
 * During which the events are held in a small limited sliding queue for short period of time, and dropped when 
 * there're no watchers dequeuing events (watching them)
 */ 
public class Journal {

	public interface MixinTemplate {
		String getPattern();
		int getArgSize();
		Kind getKind();
		
	}
	
	public enum Thread {
		Distribution,
		ShardKeeper,
		FollowerEvent,
		ClientEvent
		;
	}

	public enum Kind {
		MSG(Level.INFO), 
		ERROR(Level.ERROR), 
		FATAL(Level.ERROR)
		;
		private final Level level;
		Kind(final Level l) {
			this.level=l;
		}
	}
	
	public static enum Inbox {
		LEADER, FOLLOWER, TASKS, BROKER;
		public static Inbox fromClass(final Class<?> c) {
			final String name = c.getPackage().getName();
			Inbox i = null;
			if (name.contains(".leader")) {
				i = Inbox.LEADER;
			} else if (name.contains(".follower")) {
				i = Inbox.FOLLOWER;
			} else if (name.contains(".task")) {
				i = Inbox.TASKS;
			} else if (name.contains(".broker")) {
				i = Inbox.BROKER;
			}
			return i;
		}
	}

	private AtomicBoolean state = new AtomicBoolean();
	private Map<Inbox, BlockingQueue<String>> inboxes = new ConcurrentHashMap<>();		
	
	public synchronized Journal.JournalHandler get(final Class<?> claz) {
		final Inbox i = Inbox.fromClass(claz);
		BlockingQueue<String> inbox = inboxes.get(i);
		if (inbox==null) {
			inboxes.put(i, inbox = new ArrayBlockingQueue<>(20));
		}
		return new JournalHandler(LoggerFactory.getLogger(claz), inbox, state);
	}
	
	public void on() {
		this.state.set(true);
	}
	public void off() {
		this.state.set(false);
	}
	public BlockingQueue<String> getBy(final Inbox i) {
		return this.inboxes.get(i);
	}
	
	public static class JournalHandler {
		
		
		private final Logger logger;
		private final BlockingQueue<String> inbox;
		private final AtomicBoolean state;
		private JournalHandler(
				final Logger logger, 
				final BlockingQueue<String> inbox, 
				final AtomicBoolean state) {
			this.logger = logger;
			this.inbox = inbox;
			this.state = state;
		}
		
		public void event(final MixinTemplate msg, final Object...args) {
			if (args.length != msg.getArgSize()) {
				throw new IllegalArgumentException("");
			}
			final String msgg = MessageFormat.format(msg.getPattern(), args);
			doLog4jLog(msg.getKind().level, msgg, args);
			doTransient(msgg, args);
		}

		private void doTransient(final String msg, final Object... args) {
			if (state.get()) {
				while (!inbox.offer(msg)) {
					inbox.poll();
				}
			}
		}

		public BlockingQueue<String> getQueue() {
			return this.inbox;
		}
		
		private void doLog4jLog(final Level level, final String msg, final Object... args) {
			if (level.equals(Level.WARN) && logger.isWarnEnabled()) {
				logger.warn(msg, args);
			} else if (level.equals(Level.INFO) && logger.isInfoEnabled()) {
				logger.info(msg, args);
			} else if (level.equals(Level.DEBUG) && logger.isDebugEnabled()) {
				logger.debug(msg, args);
			} else if (level.equals(Level.ERROR) && logger.isErrorEnabled()) {
				logger.error(msg, args);
			}
		}
	}
	
}