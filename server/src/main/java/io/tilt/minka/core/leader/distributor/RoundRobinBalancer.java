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
package io.tilt.minka.core.leader.distributor;

import java.util.List;
import java.util.Set;

import io.tilt.minka.api.Config;
import io.tilt.minka.api.Pallet;
import io.tilt.minka.core.leader.PartitionTable;
import io.tilt.minka.domain.Shard;
import io.tilt.minka.domain.ShardEntity;

/**
 * 
 * @author Cristian Gonzalez
 * @since Abr 5, 2015
 */
public class RoundRobinBalancer implements Balancer {

		
		public RoundRobinBalancer(final Config config) {
			
		}
		
		/*
		 * TODO BUG: no olvidarme de evitar quitarle tareas a los q estan en
		 * cuarentena la inconsistencia es que si durante un tiempo prolongado se
		 * mantiene un server en cuarentena: promedio por los onlines, y ese nodo
		 * no reporto ninguna perdida
		 */
		public void balance(
				final Pallet<?> pallet,
				final PartitionTable table, 
				final Reallocation realloc, 
				final List<Shard> onlineShards,
				final Set<ShardEntity> creations, 
				final Set<ShardEntity> deletions, 
				final int accounted) {}

}
