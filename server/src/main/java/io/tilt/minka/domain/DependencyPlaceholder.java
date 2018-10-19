package io.tilt.minka.domain;

import org.apache.commons.lang.Validate;

import io.tilt.minka.model.PartitionDelegate;
import io.tilt.minka.model.PartitionMaster;

/**
 * Value object for information that is held or provided by the client,
 * buy required after the spring context has started.
 */
@SuppressWarnings("rawtypes")
public class DependencyPlaceholder {

	// default fallback until client is ready to deliver
	private PartitionDelegate awaitingFallbackDelegate;
	private PartitionMaster awaitingFallbackMaster;

	// client's implementation 
	private PartitionDelegate delegate;
	private PartitionMaster master;

	private PartitionDelegate consumerDelegate;
	
	public DependencyPlaceholder() {
		initDefault();
	}

	public DependencyPlaceholder(
			final PartitionDelegate delegate, 
			final PartitionMaster master) {
		// NO Validar
		this();
		this.delegate = delegate;
		this.master = master;
	}

	private void initDefault() {
		this.awaitingFallbackDelegate = (PartitionDelegate) new AwaitingDelegate();
		this.awaitingFallbackMaster = (PartitionMaster) new AwaitingDelegate();
	}

	public void setDelegate(PartitionDelegate delegate) {
		Validate.notNull(delegate);
		this.delegate = delegate;
	}

	public void setMaster(PartitionMaster master) {
		Validate.notNull(master);
		this.master = master;
	}
	
	public void setConsumerDelegate(final PartitionDelegate consumer) {
		Validate.notNull(consumer);
		Validate.isTrue(delegate==null, "You're trying a consumer delegate on an already PartitionDelegate implementation set, context");
		this.consumerDelegate = consumer;
	}

	public PartitionDelegate getConsumerDelegate() {
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