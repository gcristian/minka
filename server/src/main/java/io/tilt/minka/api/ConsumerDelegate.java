package io.tilt.minka.api;

import static io.tilt.minka.api.ConsumerDelegate.Event.activation;
import static io.tilt.minka.api.ConsumerDelegate.Event.deactivation;
import static io.tilt.minka.api.ConsumerDelegate.Event.loadduties;
import static io.tilt.minka.api.ConsumerDelegate.Event.loadpallets;
import static io.tilt.minka.api.ConsumerDelegate.Event.release;
import static io.tilt.minka.api.ConsumerDelegate.Event.releasePallet;
import static io.tilt.minka.api.ConsumerDelegate.Event.report;
import static io.tilt.minka.api.ConsumerDelegate.Event.capture;
import static io.tilt.minka.api.ConsumerDelegate.Event.capturePallet;
import static io.tilt.minka.api.ConsumerDelegate.Event.transfer;
import static io.tilt.minka.api.ConsumerDelegate.Event.transferPallet;
import static io.tilt.minka.api.ConsumerDelegate.Event.update;
import static io.tilt.minka.api.ConsumerDelegate.Event.updatePallet;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Contract bridge to encapsulate delegate implementation at Minka  
 * @author Cristian Gonzalez
 * @since Nov 8, 2016
 */
public class ConsumerDelegate<D extends Serializable, P extends Serializable> implements PartitionMaster<D, P> {

	private static final String UNMAPPED_EVENT = "{}: Unmapped event: {}";
	private static final Logger log = LoggerFactory.getLogger(MinkaClient.class);

	public enum Event {
		// consumers
		capture(true, "taking responsibilities on duties: start"), 
		release(true, "releasing responsibilities on duties: stop"), 
		capturePallet(false, "taking responsibilities on pallets: prepare"), 
		releasePallet(false, "releasing responsibilities on pallets: discard"), 
		update(false, "receiving update on duty: new payload"), 
		updatePallet(false, "receiving updates on pallets: new payload"), 
		// biconsumers
		transfer(false, "receive transferred objects to duties"),
		transferPallet(false, "receive transferred objects to pallets"),
		// runnables
		activation(false, "on minka loading context"),
		deactivation(false, "on minka unloading context"), 
		// suppliers
		report(true, "report taken duties"), 
		loadduties(true, "first even before distribution: load duties from a source"), 
		loadpallets(true, "first even before distribution: load pallets from a source"),
		;
		private final String title;
		private final boolean mandatory;
		Event(final boolean mandatory, final String title) {
			this.title = title;
			this.mandatory = mandatory;
		}
		public String getTitle() {
			return this.title;
		}
		@Override
		public String toString() {
			return name() + ":" + getTitle();
		}
		public boolean mandatory() {
			return mandatory;
		}
	}
	
	private Consumer<Duty<D>> consumerUpdate;
	private Consumer<Pallet<P>> consumerPalletUpdate;
	private BiConsumer<Duty<D>, Serializable> biconsumerTransfer;
	private BiConsumer<Pallet<P>, Serializable> biconsumerPalletTransfer;
	private final Map<Event, Consumer<Set<Pallet<P>>>> consumersPallets;
	private final Map<Event, Consumer<Set<Duty<D>>>> consumers;
	private final Map<Event, Supplier<Set<Duty<D>>>> suppliers;
	private Supplier<Set<Pallet<P>>> palletSupplier;
	private final Map<Event, Runnable> runnables;
	private final Map<Pallet<P>, Double> capacities;
	
	protected ConsumerDelegate() {
		super();
		this.consumers = new HashMap<>();
		this.consumersPallets = new HashMap<>();
		this.suppliers = new HashMap<>();
		this.runnables = new HashMap<>();
		this.capacities = new HashMap<>();
	}
	
	protected void addConsumer(final Consumer<Set<Duty<D>>> consumer, Event event) {
		Validate.notNull(consumer);
		this.consumers.put(event, consumer);
	}
	protected void addConsumerPallet(final Consumer<Set<Pallet<P>>> consumer, Event event) {
		Validate.notNull(consumer);
		this.consumersPallets.put(event, consumer);
	}
	protected void addRunnable(final Event event, Runnable runnable) {
		Validate.notNull(runnable);
		this.runnables.put(event, runnable);
	}
	protected void addPalletSupplier(final Supplier<Set<Pallet<P>>> supplier) {
		Validate.notNull(supplier);
		this.palletSupplier = supplier;
	}
	protected void addSupplier(final Event event, Supplier<Set<Duty<D>>> supplier) {
		Validate.notNull(supplier);
		this.suppliers.put(event, supplier);
	}
	protected void addCapacity(final Pallet<P> pallet, final Double weight) {
		Validate.notNull(pallet);
		Validate.notNull(weight);
		this.capacities.put(pallet, weight);
	}
	protected void addBiConsumerTransfer(final BiConsumer<Duty<D>, Serializable> biconsumerTransfer) {
		this.biconsumerTransfer = biconsumerTransfer;
	}
	protected void addBiConsumerTransferPallet(final BiConsumer<Pallet<P>, Serializable> biconsumerTransferPallet) {
		this.biconsumerPalletTransfer = biconsumerTransferPallet;
	}
	protected void addConsumerUpdate(Consumer<Duty<D>> consumerUpdate) {
		this.consumerUpdate = consumerUpdate;
	}	
	protected void addConsumerUpdatePallet(Consumer<Pallet<P>> consumerUpdate) {
		this.consumerPalletUpdate = consumerUpdate;
	}	

	// -------- interfase bridge
	
	private boolean readyIf(final Object o, Event ev) {
		if (o instanceof Map && ((Map)o).containsKey(ev)) {
			return true;
		} else {
			if (o!=null) {
				return true;
			}
		}
		log.info("{}: ConsumerDelegate not ready: still unmapped events: {}", getClass().getSimpleName(), ev);
		return false;
	}
	private boolean explicitlyReady;
	protected boolean isExplicitlyReady() {
		return this.explicitlyReady;
	}
	protected void setExplicitlyReady() {
		if (areRequiredEventsMapped()) {
			this.explicitlyReady = true;
		} else {
			throw new IllegalStateException("There're events yet to be mapped before calling load()!");
		}
	}
	
	protected boolean areRequiredEventsMapped() {
		final boolean ready = readyIf(consumers, release) &&
			readyIf(consumers, capture) &&
			readyIf(suppliers, report) &&
			readyIf(suppliers, loadduties) &&
			palletSupplier!=null;
		if (palletSupplier==null) {
			log.error("{}: ConsumerDelegate not ready: still unmapped event: Pallet supplier", 
				getClass().getSimpleName());
		}
		return ready;
	}

	public Set<Duty<D>> loadDuties() {
		Supplier<Set<Duty<D>>> s = suppliers.get(loadduties);
		if (s!=null) {
			return s.get();
		} else {
			log.error(UNMAPPED_EVENT, getClass().getSimpleName(), loadduties);
			return Collections.emptySet();
		}
	}

	public Set<Pallet<P>> loadPallets() {
		if (palletSupplier!=null) {
			return palletSupplier.get();
		} else {
			log.error(UNMAPPED_EVENT, getClass().getSimpleName(), loadpallets);
			return Collections.emptySet();
		}
	}
	
	@Override
	public boolean isReady() {
		final boolean required = areRequiredEventsMapped();
		final boolean allMapped = (required && isExplicitlyReady()) || 
				(required && !isExplicitlyReady() && 
				readyIf(consumersPallets, releasePallet) &&
				readyIf(consumersPallets, capturePallet) &&
				readyIf(consumersPallets, capturePallet) &&
				readyIf(consumerUpdate, update) &&
				readyIf(consumerPalletUpdate, updatePallet) &&
				readyIf(biconsumerTransfer, transfer) &&
				readyIf(biconsumerPalletTransfer, transferPallet) &&
				readyIf(consumers, update) &&
				readyIf(runnables, activation) &&
				readyIf(runnables, deactivation));
		if (!allMapped) {
			return false;
		} else if (palletSupplier == null) {
			log.error("{}: ConsumerDelegate not ready: still unmapped event: Pallet supplier", 
					getClass().getSimpleName());
			return false;
		} else if (capacities.isEmpty()) {
			log.error("{}: ConsumerDelegate not ready: capacities not reported", 
					getClass().getSimpleName());
			return false;
		} else if (biconsumerTransfer ==null) {
			log.error("{}: ConsumerDelegate not ready: biconsumers still unset", 
					getClass().getSimpleName());
			return false;
		} else {
			for (final Pallet<P> p: loadPallets()) {
				if (!capacities.containsKey(p)) {
					log.error("{}: Unset pallet capacity: {}", getClass().getSimpleName(), p);
					return false;
				}
			}
		}
		return true;
	}
	@Override
	public void capture(final Set<Duty<D>> duties) {
		final Consumer<Set<Duty<D>>> c = this.consumers.get(capture);
		if (c!=null) {
			c.accept(duties);
		} else {
			log.error(UNMAPPED_EVENT, getClass().getSimpleName(), capture);
		}
	}
	@Override
	public void capturePallet(final Set<Pallet<P>> pallets) {
		final Consumer<Set<Pallet<P>>> c = this.consumersPallets.get(capturePallet);
		if (c!=null) {
			c.accept(pallets);
		} else {
			log.error(UNMAPPED_EVENT, getClass().getSimpleName(), capturePallet);
		}
	}
	@Override
	public void release(Set<Duty<D>> duties) {
		final Consumer<Set<Duty<D>>> c = this.consumers.get(release);
		if (c!=null) {
			c.accept(duties);
		} else {
			log.error(UNMAPPED_EVENT, getClass().getSimpleName(), release);
		}
	}
	@Override
	public void releasePallet(final Set<Pallet<P>> pallets) {
		final Consumer<Set<Pallet<P>>> c = this.consumersPallets.get(releasePallet);
		if (c!=null) {
			c.accept(pallets);
		} else {
			log.error(UNMAPPED_EVENT, getClass().getSimpleName(), releasePallet);
		}
	}
	
	@Override
	public void update(final Duty<D> duties) {
		if (this.consumerUpdate!=null) {
			this.consumerUpdate.accept(duties);
		} else {
			log.error(UNMAPPED_EVENT, getClass().getSimpleName(), update);
		}
	}
	@Override
	public void transfer(final Duty<D> duty, Serializable clientPayload) {
		if (this.biconsumerTransfer!=null) {
			this.biconsumerTransfer.accept(duty, clientPayload);
		} else {
			log.error(UNMAPPED_EVENT, getClass().getSimpleName(), transfer);
		}
	}
	@Override
	public void update(final Pallet<P> pallet) {
		if (this.consumerPalletUpdate!=null) {
			this.consumerPalletUpdate.accept(pallet);
		} else {
			log.error(UNMAPPED_EVENT, getClass().getSimpleName(), update);
		}
	}
	@Override
	public void transfer(final Pallet<P> pallet, Serializable clientPayload) {
		if (this.biconsumerPalletTransfer!=null) {
			this.biconsumerPalletTransfer.accept(pallet, clientPayload);
		} else {
			log.error(UNMAPPED_EVENT, getClass().getSimpleName(), transferPallet);
		}
	}
	@Override
	public void activate() {
		final Runnable run = this.runnables.get(activation);
		if (run!=null) {
			run.run();;
		} else {
			log.error(UNMAPPED_EVENT, getClass().getSimpleName(), activation);
		}
	}
	@Override
	public void deactivate() {
		Runnable r = this.runnables.get(deactivation);
		if (r!=null) {
			r.run();;
		} else {
			log.error(UNMAPPED_EVENT, getClass().getSimpleName(), deactivation);
		}
	}
	@Override
	public Set<Duty<D>> reportCapture() {
		Supplier<Set<Duty<D>>> s = this.suppliers.get(report);
		if (s!=null) {
			return s.get();
		} else {
			log.error("{}: Unmapped event: {}", getClass().getSimpleName(), report);
			return Collections.emptySet();
		}
	}
	@Override
	public double getTotalCapacity(final Pallet<P> pallet) {
		Double d = this.capacities.get(pallet);
		if (d!=null) {
			return d;
		} else {
			log.error(UNMAPPED_EVENT + " pallet: {}", getClass().getSimpleName(), "capacity", pallet.getId());
			return 0;
		}
	}
}
