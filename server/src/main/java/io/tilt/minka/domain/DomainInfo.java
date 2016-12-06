
package io.tilt.minka.domain;

import java.io.Serializable;
import java.util.Set;

public class DomainInfo implements Serializable {

	private static final long serialVersionUID = -8554620809270441100L;
	
	private Set<ShardEntity> domainPallets;

	public Set<ShardEntity> getDomainPallets() {
		return this.domainPallets;
	}

	public void setDomainPallets(final Set<ShardEntity> domainPallets) {
		this.domainPallets = domainPallets;
	}
		
}
