package io.tilt.minka.api;

import java.io.Serializable;
import java.time.Instant;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Note this builder does not support large binary payloads as duties are loaded into memory and transported
 * 
 * @author Cristian Gonzalez
 * @since Nov 24, 2016
 * 
 * @param <T>	A bytes serializable payload to carry along the wire from the intake point 
 * at <code> MinkaClient.getInstance().add(...) </code> to the Shard where the host app will process it 
 */
public class DutyBuilder<T extends Serializable> {
	private final String id;
	private final String palletId;

	private T payload;
	private double weight;
	private boolean synthetic;
	private boolean lazy;
	private boolean idempotent = true;

	private DutyBuilder(final String id, final String palletId) {
		Validate.notNull(id, "A non null ID is required");
		Validate.notNull(palletId, id + ": a Pallet is mandatory");
		Validate.isTrue(id.length()<512);
		this.id = id;
		this.palletId = palletId;
	}
	
	/**
	 * Builder with an ID equals to current instant's epoc-second plus nano-seconds elapsed. 
	 * @param palletId must belong to an already created pallet
	 * @param <T> the payload type 
	 * @return a builder for first class citizen duty
	 */
	public static <T extends Serializable> DutyBuilder<T> builder(final String palletId) {
		final Instant now = Instant.now();
		return new DutyBuilder<>(
				new StringBuilder()
					.append(now.getEpochSecond())
					.append(now.getNano())
					.toString(), 
				palletId);
	}
	
	/**
	 * @param id must be a unique ID within the host app domain when distributed.
	 * @param palletId must belong to an already created pallet
	 * @param <T> the payload type 
	 * @return a builder for first class citizen duty
	 */
	public static <T extends Serializable> DutyBuilder<T> builder(final String id, final String palletId) {
		return new DutyBuilder<>(id, palletId);
	}
	/**
	 * In case is not provided, type and payload become String and param id, respectedly. 
	 * @param payload	must implement Serializable
	 * @return	the builder
	 */
	public DutyBuilder<T> with(final T payload) {
		Validate.notNull(payload, id + ": You must specify payload param or use overload builder");
		this.payload = payload;
		return this;
	}
	/**
	 * The associated processing weight at execution time for the host app.
	 * Unweighted duties cannot take benefits on pallets with weighted balancers 
	 * @param weight  a value in the same scale than its pallet's shard capacity when reported by the host app.
	 * @return	the builder 
	 */
	public DutyBuilder<T> with(final double weight) {
		Validate.isTrue(weight > 0, id + "A number greater than 0 expected for workload representing the duty");
		this.weight = weight;
		return this;
	}
	/** 
	 * lazy finalization allows a duty to be peacefully considered finished once it's presence 
	 * ceases to be reported, otherwise the leader will keep trying to restart it, 
	 * even flagging the shard as rebell if it rejects to take it and report it.
	 * This's disabled by default
	 * @return	the builder 
	 * */
	public DutyBuilder<T> withLazyFinalization() {
		this.lazy = true;
		return this;
	}
	/**
	 * idempotency allows the duty to be rebalanced, often migrated (dettached from a shard, and attach at
	 * a different one), so the distribution, balance and availability Minka policies can be guaranteed.
	 * Stationary duties are never rebalanced, they're distributed and abandoned.
	 * Used when duties cannot be paused once started or involve some static coupling to the shard. 
	 * This's disabled by default.
	 * @return	the builder
	 */
	public DutyBuilder<T> asStationary() {
		this.idempotent = false;
		return this;
	}
	/**
	 * synthetic duties are distributed to all existing shards, without balancing, 
	 * they're all the same for Minka, all CRUD operations still apply as the duty is only 1.
	 * Useful for controlling and coordination purposes.
	 * @return	the builder
	 */
	public DutyBuilder<T> asSynthetic() {
		this.synthetic = true;;
		return this;
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Duty<T> build() {
		if (weight ==0) {
			weight = 1;
		}
		if (payload == null) {
			return new Task(id, id, weight, palletId, synthetic, lazy, idempotent);								
		} else {
			return new Task<>(payload, id, weight, palletId, synthetic, lazy, idempotent);
		}
	}
		
	public static class Task<T extends Serializable> implements Duty<T>, EntityPayload {

		private static final long serialVersionUID = 4976043821619752116L;

		private final String id;
		private final String palletId;
		/* courtesy: at client assignation the duty has the pallet embedded*/
		private Pallet<?> pallet;
		private final double load;
		private final T payload;
		private Class<T> type;
		private final boolean synthetic;
		private final boolean lazy;
		private final boolean idempotent;
		private final Instant timestamp;

		@SuppressWarnings("unchecked")
		protected Task(
				final T payload, 
				final String id, 
				final double load,
				final String palletId, 
				final boolean synthetic, 
				final boolean lazy, 
				final boolean idempotent) {
			this.id = id;
			this.palletId = palletId;
			this.load = load;
			this.payload = payload;
			this.type = (Class<T>) payload.getClass();
			this.synthetic = synthetic;
			this.lazy = lazy;
			this.idempotent = idempotent;
			this.timestamp = Instant.now();
			validateBuiltParams(this);
		}

		public static void validateBuiltParams(final Duty<?> duty) {
			Validate.notNull(duty.getId(), "A non null ID is required");
			Validate.isTrue(duty.getId().length() < 128, "an entity Id maximum length of 128 chars is required");
			final String id = "Duty:" + duty.getId() + " - ";
			Validate.notNull(duty.getPalletId(), id + "a Pallet is mandatory");
			Validate.isTrue(duty.getPalletId().length() < 128, "an entity Id maximum length of 128 chars is required");
			Validate.notNull(duty.getClassType(), id + "You must specify param's class or use overload builder");
			Validate.notNull(duty.get(), id + "You must specify payload param or use overload builder");
			Validate.isTrue(duty.getWeight() > 0,
					id + "A number greater than 0 expected for workload representing the duty");
		}

		@Override
		public boolean equals(Object obj) {
			if (obj != null && obj instanceof Entity) {
				if (obj == this ) {
					return true;
				} else {
					@SuppressWarnings("unchecked")
					Entity<T> entity = (Entity<T>) obj;
					return getId().equals(entity.getId());
				}
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			return new HashCodeBuilder().append(getId()).toHashCode();
		}

		@Override
		public String toString() {
			return getId();
		}

		@Override
		public Class<T> getClassType() {
			return type;
		}

		@Override
		public T get() {
			return payload;
		}

		public Instant getTimestamp() {
			return timestamp;
		}
		
		@Override
		public double getWeight() {
			return load;
		}

		@Override
		public String getId() {
			return id;
		}

		protected void setPallet(Pallet<?> pallet) {
			this.pallet = pallet;
		}
		@Override
		public Pallet<?> getPallet() {
			return pallet;
		}

		@Override
		public String getPalletId() {
			return palletId;
		}
		
		@Override
		public boolean isLazyFinalized() {
			return lazy;
		}

		@Override
		public boolean isIdempotent() {
			return idempotent;
		}

		@Override
		public boolean isSynthetic() {
			return synthetic;
		}

	}
}