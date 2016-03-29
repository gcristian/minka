/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.tilt.minka.business.leader;

import java.util.Set;

import io.tilt.minka.api.Duty;

public interface DutyDao {

    Set<Duty<?>> loadSnapshot();
    
    void save(Duty<?> duty);
    
    void saveSnapshot(Set<Duty<?>> list);
    
    public class SpectatorDutyDao implements DutyDao {

        @Override
        public Set<Duty<?>> loadSnapshot() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void save(Duty<?> duty) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void saveSnapshot(Set<Duty<?>> list) {
            // TODO Auto-generated method stub
            
        }
        
    }
}
