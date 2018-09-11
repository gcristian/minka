package io.tilt.minka.api;

import java.io.Serializable;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.apache.commons.lang.Validate;

import io.tilt.minka.core.leader.balancer.Balancer;
import io.tilt.minka.domain.AwaitingDelegate;
import io.tilt.minka.domain.ConsumerDelegate;
import io.tilt.minka.domain.ConsumerDelegate.MappingEvent;
import io.tilt.minka.domain.DependencyPlaceholder;

/**
 * Mapper for Minka sharding events.<br> 
 * When all listening events are mapped call EventMapper::done to release bootstrap process.
 * <br>
 * Some events are mandatory and others optional, strictly related to the usage of {@linkplain Client}. <br>
 * Usually client will map capturing and releasing events for duties, and set server capacities for each pallet.
 * <br>
 * Declaring shard capacities are required if client uses a balancer that depends on duty weights. <br> <br>
 */
public class EventMapper {

	private final Server.Tenant tenant;

	protected EventMapper(final Server.Tenant tenant)  {
		this.tenant = tenant;
	}
	
	private DependencyPlaceholder getDepPlaceholder() {
		return tenant.getContext().getBean(DependencyPlaceholder.class);
	}
	
	private synchronized void initConsumerDelegate() {
		DependencyPlaceholder holder = getDepPlaceholder();
		if (holder.getMaster() == null || holder.getMaster() instanceof AwaitingDelegate) {
			final ConsumerDelegate delegate = new ConsumerDelegate(tenant.getConfig());
			holder.setDelegate(delegate);
			holder.setMaster(delegate);
		} else {
			Validate.isTrue(holder.getDelegate() instanceof ConsumerDelegate, 
					"You're trying event's consumer with an already implementation set");				
		}
	}
	/**
	 * Mandatory. In case the current shard's elected as LeaderBootstrap.
	 * Remember the supplier's source of duties must be ACID with the Client methods used to CRUD duties.
	 * As long as Minka lacks of a CAP storage facility.
	 * Note duty instances should be created only once and then references returned. 
	 * To avoid inconsistency their return must always include any additions made thru {@linkplain Client}
	 * @param supplier	to be called only at shard's election as LeaderBootstrap  
	 * @return	the event mapper builder
	 */
	public EventMapper onLoad(final Supplier<Set<Duty>> supplier) {
		initConsumerDelegate();
		((ConsumerDelegate)getDepPlaceholder().getMaster()).putSupplier(MappingEvent.loadduties, supplier);
		return this;
	}
	/**
	 * Mandatory. In case the current shard's elected as LeaderBootstrap.
	 * To avoid inconsistency their return must always include any additions made thru {@linkplain Client}
	 * @param supplier	to be called only at shard's election as LeaderBootstrap
	 * @return	the event mapper builder
	 */
	public EventMapper onPalletLoad(final Supplier<Set<Pallet>> supplier) {
		initConsumerDelegate();
		((ConsumerDelegate)getDepPlaceholder().getMaster()).setPalletSupplier(supplier);
		return this;
	}
	/**
	 * Mandatory. Map duty distribution and delegation responsibilities to a consumer 
	 * @see PartitionDelegate
	 * @param consumer	to be called anytime a distribution and balance runs in the leader shard
	 * @return	the event mapper builder
	 */
	public EventMapper onCapture(final Consumer<Set<Duty>> consumer) {
		initConsumerDelegate();
		((ConsumerDelegate)getDepPlaceholder().getDelegate()).putConsumer(consumer, MappingEvent.capture);
		return this;
	}
	/**
	 * Required optional. Map pallet assignation responsibilities to a consumer 
	 * @see PartitionDelegate
	 * @param consumer	to be called anytime a distribution and balance runs in the leader shard
	 * @return	the event mapper builder
	 */
	public EventMapper onPalletCapture(final Consumer<Set<Pallet>> consumer) {
		initConsumerDelegate();
		((ConsumerDelegate)getDepPlaceholder().getDelegate()).putConsumerPallet(consumer, MappingEvent.capturePallet);
		return this;
	}	
	/**
	 * Mandatory. Map duty release contract to a consumer 
	 * @see PartitionDelegate
	 * @param consumer	to be called anytime a distribution and balance runs in the leader shard
	 * @return	the event mapper builder
	 */
	public EventMapper onRelease(final Consumer<Set<Duty>> consumer) {
		initConsumerDelegate();
		((ConsumerDelegate)getDepPlaceholder().getDelegate()).putConsumer(consumer, MappingEvent.release);
		return this;
	}
	/**
	 * Required optional. Map pallet release contract to a consumer 
	 * @see PartitionDelegate
	 * @param consumer	to be called anytime a distribution and balance runs in the leader shard
	 * @return	the event mapper builder
	 */
	public EventMapper onPalletRelease(final Consumer<Set<Pallet>> consumer) {
		initConsumerDelegate();
		((ConsumerDelegate)getDepPlaceholder().getDelegate()).putConsumerPallet(consumer, MappingEvent.releasePallet);
		return this;
	}
	/**
	 * Optional. Map an update on the duty's payload to a consumer
	 * @param consumer	to be called only on client's call thru Client.update(..)
	 * @return	the event mapper builder
	 */
	public EventMapper onUpdate(final Consumer<Duty> consumer) {
		initConsumerDelegate();
		((ConsumerDelegate)getDepPlaceholder().getDelegate()).setConsumerUpdate(consumer);
		return this;
	}
	/**
	 * Optional. Map an update on the pallet's payload to a consumer
	 * @param consumer	to be called only on client's call thru MinkaClient.update(..)
	 * @return	the event mapper builder
	 */
	public EventMapper onPalletUpdate(final Consumer<Pallet> consumer) {
		initConsumerDelegate();
		((ConsumerDelegate)getDepPlaceholder().getDelegate()).setConsumerUpdatePallet(consumer);
		return this;
	}
	/**
	 * Optional. Map duty object's transfer responsibilities to a receptionist consumer 
	 * @see PartitionDelegate
	 * @param biconsumer	to be called only on client's call thru MinkaClient.deliver(...)
	 * @return	the event mapper builder
	 */
	public EventMapper onTransfer(final BiConsumer<Duty, Serializable> biconsumer) {
		initConsumerDelegate();
		((ConsumerDelegate)getDepPlaceholder().getDelegate()).setBiConsumerTransfer(biconsumer);
		return this;
	}
	public EventMapper onPalletTransfer(final BiConsumer<Pallet, Serializable> biconsumer) {
		initConsumerDelegate();
		((ConsumerDelegate)getDepPlaceholder().getDelegate()).setBiConsumerTransferPallet(biconsumer);
		return this;
	}
	/**
	 * Optional. Adds a custom balancer
	 * @param balancer	to use at balancing phase
	 * @return	the event mapper builder
	 */
	public EventMapper onBalance(final Balancer balancer) {
		Validate.notNull(balancer);
		Balancer.Directory.addCustomBalancer(balancer);
		return this;
	}
	/**
	 * Optional. Map minka's service start to a consumer
	 * @param runnable callback to run at event dispatch
	 * @return	the event mapper builder
	 */
	public EventMapper onActivation(final Runnable runnable) {
		initConsumerDelegate();
		((ConsumerDelegate)getDepPlaceholder().getDelegate()).putRunnable(MappingEvent.activation, runnable);
		return this;
	}
	/**
	 * Optional. Map minka's service shutdown to a consumer. 
	 * @param runnable	callback to run at event dispatch
	 * @return	the event mapper builder
	 */
	public EventMapper onDeactivation(final Runnable runnable) {
		initConsumerDelegate();
		((ConsumerDelegate)getDepPlaceholder().getDelegate()).putRunnable(MappingEvent.deactivation, runnable);
		return this;
	}
	/**
	 * Mandatory for pallets using a weighted-balancer to be actually distributed. <br> 
	 * Explicitly set current shard's capacity for a given Pallet.
	 * Weight value's measure is not validated by Minka against duty weight measure.  
	 * @param weight 	must be in the same measure than duty weights grouped by this pallet
	 * @param pallet	the pallet to report capacity about
	 * @return	the event mapper builder
	 */
	public EventMapper setCapacity(final Pallet pallet, final double weight) {
		initConsumerDelegate();
		((ConsumerDelegate)getDepPlaceholder().getDelegate()).putCapacity(pallet, weight);
		return this;
	}

	/**
	 * Tells the Server to release the bootstrapping process that fires leader and follower agents. <br>
	 * Mandatory when mapping only required events like: duties::capture/release. 
	 * Not required when mapping all events, which will release the bootstrap by itself.
	 * @return	the event mapper builder
	 */
	public EventMapper done() {
		initConsumerDelegate();
		((ConsumerDelegate)getDepPlaceholder().getDelegate()).setExplicitlyReady();
		return this;
	}

}
