package io.tilt.minka.api;

import static io.tilt.minka.api.ConsumerDelegate.Event.activation;
import static io.tilt.minka.api.ConsumerDelegate.Event.deactivation;
import static io.tilt.minka.api.ConsumerDelegate.Event.loadduties;
import static io.tilt.minka.api.ConsumerDelegate.Event.loadpallets;
import static io.tilt.minka.api.ConsumerDelegate.Event.reception;
import static io.tilt.minka.api.ConsumerDelegate.Event.release;
import static io.tilt.minka.api.ConsumerDelegate.Event.releasePallet;
import static io.tilt.minka.api.ConsumerDelegate.Event.report;
import static io.tilt.minka.api.ConsumerDelegate.Event.take;
import static io.tilt.minka.api.ConsumerDelegate.Event.takePallet;
import static io.tilt.minka.api.ConsumerDelegate.Event.update;

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
 * Facility to isolate consumer usage instead of interfase implementation usage, at minka.  
 * @author Cristian Gonzalez
 * @since Nov 8, 2016
 */
public class ConsumerDelegate<D extends Serializable, P extends Serializable> implements PartitionMaster<D, P> {

	private static final String UNMAPPED_EVENT = "{}: Unmapped event: {}";
	private static final Logger log = LoggerFactory.getLogger(MinkaClient.class);

	public enum Event {
		take, release, takePallet, releasePallet, update, // consumners
		notify, reception, // biconsumers
		activation, deactivation, // runnables
		report, loadduties, loadpallets // suppliers
	}
	
	private Consumer<Duty<D>> consumerUpdate;
	private BiConsumer<Duty<D>, Serializable> biconsumerDeliver;
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
	
	public void addConsumer(Consumer<Set<Duty<D>>> consumer, Event event) {
		Validate.notNull(consumer);
		this.consumers.put(event, consumer);
	}
	public void addConsumerPallet(Consumer<Set<Pallet<P>>> consumer, Event event) {
		Validate.notNull(consumer);
		this.consumersPallets.put(event, consumer);
	}
	public void addRunnable(Event event, Runnable runnable) {
		Validate.notNull(runnable);
		this.runnables.put(event, runnable);
	}
	public void addPalletSupplier(Supplier<Set<Pallet<P>>> supplier) {
		Validate.notNull(supplier);
		this.palletSupplier = supplier;
	}
	public void addSupplier(Event event, Supplier<Set<Duty<D>>> supplier) {
		Validate.notNull(supplier);
		this.suppliers.put(event, supplier);
	}
	public void addCapacity(Pallet<P> pallet, final Double weight) {
		Validate.notNull(pallet);
		Validate.notNull(weight);
		this.capacities.put(pallet, weight);
	}
	public void addBiConsumerDeliver(BiConsumer<Duty<D>, Serializable> biconsumerDeliver) {
		this.biconsumerDeliver = biconsumerDeliver;
	}
	public void addConsumerUpdate(Consumer<Duty<D>> onsumerUpdate) {
		this.consumerUpdate = onsumerUpdate;
	}	
	// -------- interfase bridge
	
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
	
	private boolean readyIf(Map<Event, ?> map, Event ev) {
		if (map.containsKey(ev)) {
			return true;
		}
		log.info("{}: ConsumerDelegate not ready: still unmapped events: {}", 
				getClass().getSimpleName(), ev);
		return false;
	}
	private boolean explicitlyReady;
	protected boolean isExplicitlyReady() {
		return this.explicitlyReady;
	}
	public void setExplicitlyReady() {
		if (areRequiredEventsMapped()) {
			this.explicitlyReady = true;
		} else {
			throw new IllegalStateException("There're events yet to be mapped before calling load()!");
		}
	}
	
	protected boolean areRequiredEventsMapped() {
		final boolean ready = readyIf(consumers, release) &&
			readyIf(consumers, take) &&
			readyIf(suppliers, report) &&
			readyIf(suppliers, loadduties) &&
			palletSupplier!=null;
		if (palletSupplier==null) {
			log.error("{}: ConsumerDelegate not ready: still unmapped event: Pallet supplier", 
				getClass().getSimpleName());
		}
		return ready;
	}
	
	@Override
	public boolean isReady() {
		final boolean required = areRequiredEventsMapped();
		final boolean allMapped = (required && isExplicitlyReady()) || 
				(required && !isExplicitlyReady() && 
				readyIf(consumersPallets, releasePallet) &&
				readyIf(consumersPallets, takePallet) &&
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
		} else if (biconsumerDeliver ==null) {
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
	public void take(Set<Duty<D>> duties) {
		final Consumer<Set<Duty<D>>> c = this.consumers.get(take);
		if (c!=null) {
			c.accept(duties);
		} else {
			log.error(UNMAPPED_EVENT, getClass().getSimpleName(), take);
		}
	}
	@Override
	public void takePallet(Set<Pallet<P>> pallets) {
		final Consumer<Set<Pallet<P>>> c = this.consumersPallets.get(takePallet);
		if (c!=null) {
			c.accept(pallets);
		} else {
			log.error(UNMAPPED_EVENT, getClass().getSimpleName(), takePallet);
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
	public void releasePallet(Set<Pallet<P>> pallets) {
		final Consumer<Set<Pallet<P>>> c = this.consumersPallets.get(releasePallet);
		if (c!=null) {
			c.accept(pallets);
		} else {
			log.error(UNMAPPED_EVENT, getClass().getSimpleName(), releasePallet);
		}
	}
	
	@Override
	public void update(Duty<D> duties) {
		if (this.consumerUpdate!=null) {
			this.consumerUpdate.accept(duties);
		} else {
			log.error(UNMAPPED_EVENT, getClass().getSimpleName(), update);
		}
	}
	@Override
	public void deliver(Duty<D> duty, Serializable clientPayload) {
		if (this.biconsumerDeliver!=null) {
			this.biconsumerDeliver.accept(duty, clientPayload);
		} else {
			log.error(UNMAPPED_EVENT, getClass().getSimpleName(), reception);
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
	public Set<Duty<D>> reportTaken() {
		Supplier<Set<Duty<D>>> s = this.suppliers.get(report);
		if (s!=null) {
			return s.get();
		} else {
			log.error("{}: Unmapped event: {}", getClass().getSimpleName(), report);
			return Collections.emptySet();
		}
	}
	@Override
	public double getTotalCapacity(Pallet<P> pallet) {
		Double d = this.capacities.get(pallet);
		if (d!=null) {
			return d;
		} else {
			log.error(UNMAPPED_EVENT + " pallet: {}", getClass().getSimpleName(), "capacity", pallet.getId());
			return 0;
		}
	}
}
