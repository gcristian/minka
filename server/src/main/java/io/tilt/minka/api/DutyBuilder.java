package io.tilt.minka.api;

import java.io.Serializable;

import org.apache.commons.lang.Validate;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * This builder does not support large binary payloads as duties are loaded into
 * memory and transported
 */
public class DutyBuilder<T extends Serializable> {
	private final String id;
	private final String palletId;

	private T value;
	private Class<T> type;
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
	 * @param id must be a unique ID within the host app domain when distributed.
	 * @param palletId must belong to an already created pallet 
	 * @return a builder for first class citizen duty
	 */
	public static <T extends Serializable> DutyBuilder<T> builder(final String id, final String palletId) {
		return new DutyBuilder<>(id, palletId);
	}
	/**
	 * In case is not provided, type and payload become String and param id, respectedly. 
	 * @param type		must extendd Serializable
	 * @param payload	must implement Serializable
	 * @return
	 */
	public DutyBuilder<T> with(final Class<T> type, final T payload) {
		Validate.notNull(type, id + ": You must specify param's class or use overload builder");
		Validate.notNull(payload, id + ": You must specify payload param or use overload builder");
		this.type = type;
		this.value = payload;
		return this;
	}
	/**
	 * The associated processing weight at execution time for the host app.
	 * Unweighted duties cannot take benefits on pallets with weighted balancers 
	 * @param weight  a value in the same scale than its pallet's shard capacity when reported by the host app. 
	 */
	public DutyBuilder<T> with(final double weight) {
		Validate.isTrue(weight > 0, id + "A number greater than 0 expected for workload representing the duty");
		this.weight = weight;
		return this;
	}
	public DutyBuilder<T> withLazyFinalization() {
		this.lazy = true;
		return this;
	}
	public DutyBuilder<T> asIdempotent() {
		this.idempotent = false;
		return this;
	}
	public DutyBuilder<T> asSynthetic() {
		this.synthetic = true;;
		return this;
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public Duty<T> build() {
		if (weight ==0) {
			weight = 1;
		}
		if (type == null) {
			return new Chore(String.class, id, id, weight, palletId, synthetic, lazy, idempotent);								
		} else {
			return new Chore<>(type, value, id, weight, palletId, synthetic, lazy, idempotent);
		}
	}
		
	public static class Chore<T extends Serializable> implements Duty<T>, EntityPayload {

		private static final long serialVersionUID = 4976043821619752116L;

		private final String id;
		private final String palletId;
		/* courtesy: at client assignation the duty has the pallet embedded*/
		private Pallet<?> pallet;
		private final double load;
		private final T value;
		private Class<T> type;
		private final boolean synthetic;
		private final boolean lazy;
		private final boolean idempotent;

		protected Chore(final Class<T> class1, final T payload, final String id, final double load,
				final String palletId, final boolean synthetic, final boolean lazy, final boolean idempotent) {
			this.id = id;
			this.palletId = palletId;
			this.load = load;
			this.value = payload;
			this.type = class1;
			this.synthetic = synthetic;
			this.lazy = lazy;
			this.idempotent = idempotent;
			validateBuiltParams(this);
		}

		public static void validateBuiltParams(final Duty<?> duty) {
			Validate.notNull(duty.getId(), "A non null ID is required");
			final String id = "Duty:" + duty.getId() + " - ";
			Validate.notNull(duty.getPalletId(), id + "a Pallet is mandatory");
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
			return value;
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