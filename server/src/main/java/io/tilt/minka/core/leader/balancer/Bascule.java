/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.tilt.minka.core.leader.balancer;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.Validate;

import io.tilt.minka.api.Duty;

/**
 * for measuring, lifting, and controlling duty load on limited capacities
 * 
 * @author Cristian Gonzalez
 * @since Nov 12, 2016
 */
public class Bascule<O, C> {
	private final O owner;
	private final double maxRealCapacity;
	
	private double maxTestWeight;
	private double liftedWeight;
	private final Set<C> cargo;
	private final Set<C> discarded;
	
	/* weighing mode bascule: only to lift weights without assigning */
	public Bascule() {
		this(null, -1, 0);
	}
	/* testing mode to assign duties to shards */
	public Bascule(final O owner, final double maxRealCapacity) {
		this(owner, maxRealCapacity, 0);
		Validate.notNull(owner);
	}
	private Bascule(final O owner, final double maxRealCapacity, int a) {
		this.owner = owner;
		this.maxRealCapacity = maxRealCapacity;
		this.cargo = new HashSet<>();
		this.discarded = new HashSet<>();
	}
	/**
	 * @param duty a testing entity 
	 * @return whether or not this bascule is able to lift the duty's weight */
	public boolean fits(final Duty<?> duty) {
		Validate.notNull(duty);
		validOperation();
		return duty.getWeight()<=maxRealCapacity;
	}
	private void validOperation() {
		if (maxRealCapacity == -1) {
			throw new IllegalStateException("bad operation: bascule in weighing mode has no capacity set !");
		}
	}
	/**
	 * @param element the cargo element
	 * @param weight the weight
	 * @return whether or not the bascule added the duty and max test weight wasnt reached yet */
	public boolean testAndLift(final C element, final double weight) {
		if (getMaxTestWeight()<0) {
			throw new IllegalStateException("bad operation: try setting the test capacity first");
		}
		return lift_(element, weight, getMaxTestWeight());
	}
	/** 
	 * @param element the cargo element
	 * @param weight the weight
	 * @return whether or not the bascule added the duty and real max capacity wasnt reached */
	public boolean tryLift(final C element, final double weight) {
		return lift_(element, weight, getMaxRealCapacity());
	}
	private boolean lift_(final C element, double weight, double max) {
		Validate.notNull(element);
		if (liftedWeight + weight > max ) {
			discarded.add(element);
			return false;
		} else {
			liftedWeight+=weight;
			cargo.add(element);
		}
		return true;
	}
	/** @return the elements failed to lift */
	public Set<C> getDiscarded() {
		return this.discarded;
	}
	/** @return the elements successfully lifted */
	public Set<C> getCargo() {
		return this.cargo;
	}
	public boolean isEmpty() {
		return this.cargo.isEmpty();
	}
	public void lift(double weight) {
		if (maxRealCapacity!=-1) {
			throw new IllegalStateException("bad operation: bascule in testing mode cannot lift unidentified weight");
		}
		liftedWeight+=weight;
	}
	public O getOwner() {
		return this.owner;
	}
	public double totalLift() {
		return liftedWeight;
	}
	public void setMaxTestingCapacity(final double maxTestWeight) {
		Validate.isTrue(maxTestWeight <= maxRealCapacity, "cannot be greater than real maximum weight capacity");
		this.maxTestWeight = maxTestWeight;
	}
	public double getMaxRealCapacity() {
		validOperation();
		return this.maxRealCapacity;
	}
	public double getMaxTestWeight() {
		validOperation();
		return this.maxTestWeight;
	}
	public static <I,C> double getMaxRealCapacity(final Set<Bascule<I,C>> all) {
		Validate.notEmpty(all);
		double total = 0;
		for (Bascule<I,C> b: all) {
			total+=b.getMaxRealCapacity();
		}
		return total;
	}
}