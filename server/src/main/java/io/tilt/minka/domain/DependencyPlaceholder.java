package io.tilt.minka.domain;

import java.io.Serializable;

import org.apache.commons.lang.Validate;

import io.tilt.minka.api.PartitionDelegate;
import io.tilt.minka.api.PartitionMaster;

/**
 * Value object for information that is held or provided by the client,
 * buy required after the spring context has started.
 */
@SuppressWarnings("rawtypes")
public class DependencyPlaceholder {

	// default fallback until client is ready to deliver
	private PartitionDelegate<? extends Serializable, ? extends Serializable> awaitingFallbackDelegate;
	private PartitionMaster<? extends Serializable, ? extends Serializable> awaitingFallbackMaster;

	// client's implementation 
	private PartitionDelegate<? extends Serializable, ? extends Serializable> delegate;
	private PartitionMaster<? extends Serializable, ? extends Serializable> master;

	private PartitionDelegate<? extends Serializable, ? extends Serializable> consumerDelegate;
	
	public DependencyPlaceholder() {
		initDefault();
	}

	public DependencyPlaceholder(
			final PartitionDelegate<? extends Serializable, ? extends Serializable> delegate, 
			final PartitionMaster<? extends Serializable, ? extends Serializable> master) {
		// NO Validar
		this();
		this.delegate = delegate;
		this.master = master;
	}

	private void initDefault() {
		this.awaitingFallbackDelegate = (PartitionDelegate<? extends Serializable, 
				? extends Serializable>) new AwaitingDelegate();
		this.awaitingFallbackMaster = (PartitionMaster<? extends Serializable, 
				? extends Serializable>) new AwaitingDelegate();
	}

	public void setDelegate(PartitionDelegate<? extends Serializable, ? extends Serializable> delegate) {
		Validate.notNull(delegate);
		this.delegate = delegate;
	}

	public void setMaster(PartitionMaster<? extends Serializable, ? extends Serializable> master) {
		Validate.notNull(master);
		this.master = master;
	}
	
	public void setConsumerDelegate(final PartitionDelegate<? extends Serializable, ? extends Serializable> consumer) {
		Validate.notNull(consumer);
		Validate.isTrue(delegate==null, "You're trying a consumer delegate on an already PartitionDelegate implementation set, context");
		this.consumerDelegate = consumer;
	}

	public PartitionDelegate<? extends Serializable, ? extends Serializable> getConsumerDelegate() {
		return this.consumerDelegate;
	}

	public PartitionDelegate getDelegate() {
		if (consumerDelegate==null) {
			if (delegate == null) {
				return awaitingFallbackDelegate;
			} else {
				return delegate;
			}
		} else {
			return consumerDelegate;
		}
	}

	public PartitionMaster getMaster() {
		if (delegate == null) {
			return awaitingFallbackMaster;
		} else {
			return master;
		}
	}

}