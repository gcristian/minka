
package io.tilt.minka.domain;

import java.io.Serializable;
import java.util.Collection;

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
