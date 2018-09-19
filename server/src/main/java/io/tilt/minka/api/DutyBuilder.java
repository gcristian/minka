package io.tilt.minka.api;

import java.io.InputStream;
import java.time.Instant;
import java.util.function.Supplier;

import org.apache.commons.lang.Validate;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Note this builder does not support large binary payloads as duties are loaded into memory and transported
 * 
 * @author Cristian Gonzalez
 * @since Nov 24, 2016
 * 
 * @param <T>	A bytes serializable payload to carry along the wire from the intake point 
 * at <code> Client.getInstance().add(...) </code> to the Shard where the host app will process it 
 */
public class DutyBuilder {
	private final String id;
	private final String palletId;

	private Supplier<InputStream> payload;
	private double weight;

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
	protected static DutyBuilder builder(final String palletId) {
		final Instant now = Instant.now();
		return new DutyBuilder(
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
	protected static DutyBuilder builder(final String id, final String palletId) {
		return new DutyBuilder(id, palletId);
	}
	/**
	 * In case is not provided, type and payload become String and param id, respectedly. 
	 * @param payload	must implement Serializable
	 * @return	the builder
	 */
	public DutyBuilder with(final Supplier<InputStream> is) {
		Validate.notNull(payload, id + ": You must specify payload param or use overload builder");
		this.payload = is;
		return this;
	}
	/**
	 * The associated processing weight at execution time for the host app.
	 * Unweighted duties cannot take benefits on pallets with weighted balancers 
	 * @param weight  a value in the same scale than its pallet's shard capacity when reported by the host app.
	 * @return	the builder 
	 */
	public DutyBuilder with(final double weight) {
		Validate.isTrue(weight > 0, id + "A number greater than 0 expected for workload representing the duty");
		this.weight = weight;
		return this;
	}
	
	public Duty build() {
		if (weight ==0) {
			weight = 1;
		}
		return new Task(payload, id, weight, palletId);
	}
		
	public static class Task implements Duty, EntityPayload {

		private static final long serialVersionUID = 4976043821619752116L;

		private final String id;
		private final String palletId;
		/* courtesy: at client assignation the duty has the pallet embedded*/
		private Pallet pallet;
		private final double load;
		private final Supplier<InputStream> payload;
		private final Instant timestamp;

		protected Task(
				final Supplier<InputStream> payload, 
				final String id, 
				final double load,
				final String palletId) {
			this.id = id;
			this.palletId = palletId;
			this.load = load;
			this.payload = payload;
			this.timestamp = Instant.now();
			validateBuiltParams(this);
		}

		public static void validateBuiltParams(final Duty duty) {
			Validate.notNull(duty.getId(), "A non null ID is required");
			Validate.isTrue(duty.getId().length() < 128, "an entity Id maximum length of 128 chars is required");
			final String id = new StringBuilder("Duty:").append(duty.getId()).append(" - ").toString();
			Validate.notNull(duty.getPalletId(), id + "a Pallet is mandatory");
			Validate.isTrue(duty.getPalletId().length() < 128, "an entity Id maximum length of 128 chars is required");
			Validate.isTrue(duty.getWeight() > 0,
					id + "A number greater than 0 expected for workload representing the duty");
		}

		@Override
		public boolean equals(Object obj) {
			if (obj != null && obj instanceof Entity) {
				if (obj == this ) {
					return true;
				} else {
					Entity entity = (Entity) obj;
					return getId().equals(entity.getId());
				}
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int res = 1;
			res *= prime + (id==null ? 1 : id.hashCode());
			return res;
		}

		@Override
		public String toString() {
			return getId();
		}

		@JsonIgnore
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

		protected void setPallet(Pallet pallet) {
			this.pallet = pallet;
		}
		@Override
		public Pallet getPallet() {
			return pallet;
		}

		@Override
		public String getPalletId() {
			return palletId;
		}

	}
}