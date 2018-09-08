package io.tilt.minka.api.config;

import io.tilt.minka.core.leader.balancer.Balancer.Strategy;
import io.tilt.minka.core.leader.balancer.FairWeightToCapacity.Dispersion;
import io.tilt.minka.core.leader.balancer.PreSort;
import io.tilt.minka.core.leader.balancer.ProgressiveSpillOver.MaxUnit;

public class BalancerConfiguration {
	public static final Strategy STRATEGY = Strategy.EVEN_WEIGHT;
	private Strategy strategy;

	public static final int EVEN_SIZE_MAX_DUTIES_DELTA_BETWEEN_SHARDS = 1;
	private int roundRobinMaxDutiesDeltaBetweenShards;
	
	public static final PreSort EVEN_WEIGHT_PRESORT = PreSort.WEIGHT;
	private PreSort evenLoadPresort;
	
	public static final MaxUnit SPILL_OVER_MAX_UNIT = MaxUnit.USE_CAPACITY;
	private MaxUnit spillOverMaxUnit;
	public static final double SPILL_OVER_MAX_VALUE = 99999999999d;
	private double spillOverMaxValue;
	
	public static final Dispersion FAIR_WEIGHT_DISPERSION = Dispersion.EVEN;
	public static final PreSort FAIR_WEIGHT_PRESORT = PreSort.DATE;

	
	public int getRoundRobinMaxDutiesDeltaBetweenShards() {
		return this.roundRobinMaxDutiesDeltaBetweenShards;
	}
	public void setRoundRobinMaxDutiesDeltaBetweenShards(int roundRobinMaxDutiesDeltaBetweenShards) {
		this.roundRobinMaxDutiesDeltaBetweenShards = roundRobinMaxDutiesDeltaBetweenShards;
	}
	public Strategy getStrategy() {
		return this.strategy;
	}
	public void setStrategy(Strategy distributorbalancerStrategy) {
		this.strategy = distributorbalancerStrategy;
	}
	public PreSort getEvenLoadPresort() {
		return this.evenLoadPresort;
	}
	public void setEvenLoadPresort(PreSort fairLoadPresort) {
		this.evenLoadPresort = fairLoadPresort; 
	}
	public MaxUnit getSpillOverMaxUnit() {
		return this.spillOverMaxUnit;
	}
	public void setSpillOverStrategy(MaxUnit spillOverStrategy) {
		this.spillOverMaxUnit = spillOverStrategy;
	}
	public double getSpillOverMaxValue() {
		return this.spillOverMaxValue;
	}
	public void setSpillOverMaxValue(double spillOverMaxValue) {
		this.spillOverMaxValue = spillOverMaxValue;
	}
	
}