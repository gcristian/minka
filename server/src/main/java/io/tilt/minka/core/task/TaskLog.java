package io.tilt.minka.core.task;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.Validate;

/**
 * A Journal facility for Stories (State changes, Isolated events, broker messages)
 * Distributed, async. and non-linear systems: are Hard 2 debug (no news here)
 *  
 * Source for sys-admin alarms, and live queries.
 * Stories should be serialized and written to disk, nothing kept in memory.
 * 
 * Usage: for a latter UI - no CMDL warriors here
 * 
 * @author Cristian Gonzalez
 * @since Aug 9, 2016
 */
public interface TaskLog {

	/* each journal matches an id based on creation ISODATE */
	Date getCreation();

	/* write a new story */
	<T> void commit(Story<T> story);

	/* read stories creation-ordered for a certain fact */
	List<Story<?>> readOrdered(Fact fact);

	/* read all stories */
	List<Story<?>> readOrdered();

	public interface JournalStorage {
		List<Date> listCreations();

		TaskLog get(Date creation);
	}

	/* something for auto-diagnose ? */
	public interface JournalChecker {
		boolean hasWarningsIssued();

		boolean hasWarningsUnsolved();

		boolean hasFatalProblems();
	}

	public class Stage {

	}

	public enum Fact {
		bootstrapper_start,
		bootstrapper_stop,
		leader_bootup(Retention.LAST, 1, -1),
		follower_bootup(Retention.LAST, 1, -1),
		leadership_candidate(Retention.SERIE, 10, -1),
		leadership_acquired,
		leadership_change,

		balancing,
		distribution_reallocation,
		distribution_reallocation_retry,
		distribution_reallocation_expired,
		distribution_duty_lost,

		proctorizing,
		proctor_inbalance,
		proctor_shard_online,
		proctor_shard_offline,
		proctor_shard_changing_state,
		shard_finding_address,

		auditory_inconsistency,
		auditory_rebell_shard,
		auditory_duty_confirmed,

		broker_disconnection,
		broker_reconection,
		broker_message_in,
		broker_message_out,

		partition_table_lost,
		follower_heartbeat,
		leader_clearance,
		follower_stuck,
		follower_polices_on,
		partition_assignation_lost,

		scheduler_scheduling,
		scheduler_run,
		scheduler_stop,
		scheduler_forward,
		scheduler_red,

		entity_created,
		entity_removed,
		entity_event,

		;
		Fact(Retention r, int maxSize, int maxAgeMs) {
			this.maxAgeMs = maxAgeMs;
			this.maxSize = maxSize;
			this.retention = r;
		}

		Fact() {
			this(Retention.SERIE, -1, 01);
		}

		private final int maxAgeMs;
		private final int maxSize;
		private final Retention retention;

		public int getMaxAgeMs() {
			return this.maxAgeMs;
		}

		public int getMaxSize() {
			return this.maxSize;
		}

		public Retention getRetention() {
			return this.retention;
		}
	}

	public interface Story<T> {
		long getStart();

		long getEnd();

		long getGlobalNumber();

		long getFactNumber();

		String getFootprint();

		Class<?> getSource();

		Fact getFact();

		Fact getIssuedFact();

		Case getCase();

		Result getResult();

		String getLegend();

		T getPayload();

		boolean hasException();

		Exception getException();
	}

	public enum Retention {
		LAST, SERIE
	}

	public enum Result {
		SUCCESS, FAILURE
	}

	public enum Case {
		ISSUED, // alias open or waiting for result
		SOLVED, // alias closed or solved 
		FINAL // alias no result
	}

	public static class StoryBuilder<T> implements Story<T> {

		protected static final AtomicLong numerator = new AtomicLong();
		protected static final Map<Fact, AtomicLong> numeratorMap = new HashMap<>();

		private final long start;
		private long end;
		private final long globalNumber;
		private final long factNumber;
		private final Class<?> clazz;
		private final Fact fact;

		private String footprint;
		private Fact issuedFact;
		private Case caze;
		private Result result;
		private String legend;
		private T payload;
		private Exception exception;

		private StoryBuilder(final Class<?> clazz, final Fact fact) {
			this.start = System.currentTimeMillis();
			this.globalNumber = 123l;
			this.factNumber = 123l;
			this.fact = fact;
			this.clazz = clazz;
		}

		public static <T> StoryBuilder<T> compose(final Class<?> clazz, final Fact fact) {
			Validate.notNull(fact);
			return new StoryBuilder<>(clazz, fact);
		}

		public StoryBuilder<T> with(final Case caze) {
			Validate.notNull(caze);
			this.caze = caze;
			return this;
		}

		public StoryBuilder<T> with(final Result result) {
			Validate.notNull(result);
			this.result = result;
			return this;
		}

		public StoryBuilder<T> with(final String legend) {
			Validate.notNull(legend);
			this.legend = legend;
			return this;
		}

		public StoryBuilder<T> with(final T payload) {
			Validate.notNull(payload);
			this.payload = payload;
			return this;
		}

		public StoryBuilder<T> with(final Exception e) {
			Validate.notNull(e);
			this.exception = e;
			return this;
		}

		public Story<T> build() {
			return this;
		}

		public long getStart() {
			return this.start;
		}

		public long getEnd() {
			return this.end;
		}

		public long getGlobalNumber() {
			return this.globalNumber;
		}

		public long getFactNumber() {
			return this.factNumber;
		}

		public Fact getFact() {
			return this.fact;
		}

		public Case getCase() {
			return caze;
		}

		public Result getResult() {
			return this.result;
		}

		public String getLegend() {
			return this.legend;
		}

		public T getPayload() {
			return this.payload;
		}

		public Fact getIssuedFact() {
			return this.issuedFact;
		}

		public String getFootprint() {
			return this.footprint;
		}

		@Override
		public boolean hasException() {
			return exception != null;
		}

		@Override
		public Exception getException() {
			return exception;
		}

		@Override
		public Class<?> getSource() {
			return clazz;
		}
	}

}
