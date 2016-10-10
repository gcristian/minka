package io.tilt.minka.api;

@SuppressWarnings("rawtypes")
public class DependencyPlaceholder {

	private PartitionDelegate<?, ?> delegate;
	private PartitionMaster<?, ?> master;

	private PartitionDelegate<?, ?> awaitingFallbackDelegate;
	private PartitionMaster<?, ?> awaitingFallbackMaster;

	public DependencyPlaceholder() {
		initDefault();
	}

	public DependencyPlaceholder(final PartitionDelegate<?, ?> delegate, final PartitionMaster<?, ?> master) {
		// NO Validar
		this();
		this.delegate = delegate;
		this.master = master;
	}

	private void initDefault() {
		this.awaitingFallbackDelegate = new AwaitingDelegate();
		this.awaitingFallbackMaster = new AwaitingDelegate();
	}

	public void setDelegate(PartitionDelegate<?, ?> delegate) {
		this.delegate = delegate;
	}

	public void setMaster(PartitionMaster<?, ?> master) {
		this.master = master;
	}

	public PartitionDelegate getDelegate() {
		if (delegate == null) {
			return awaitingFallbackDelegate;
		} else {
			return delegate;
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