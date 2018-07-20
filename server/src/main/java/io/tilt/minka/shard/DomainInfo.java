
package io.tilt.minka.shard;

import java.io.Serializable;
import java.util.Collection;

import io.tilt.minka.domain.ShardEntity;

public class DomainInfo implements Serializable {

	private static final long serialVersionUID = -8554620809270441100L;
	
	private Collection<ShardEntity> domainPallets;

	public Collection<ShardEntity> getDomainPallets() {
		return this.domainPallets;
	}

	public void setDomainPallets(final Collection<ShardEntity> domainPallets) {
		this.domainPallets = domainPallets;
	}
		
}
