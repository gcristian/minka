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
package io.tilt.minka.domain;

/**
 * A partitioning event onto a sharding duty.
 * Later this value will hop from one to another as a cycle or recycle
 * depending on the State value. 
 */
public enum DutyEvent {
    /* user creates a duty from PartitionService */
    CREATE(true),
    /* leader assigns to a Shard */
    ASSIGN(false),
    /* user updates something related to the duty that leader must notify the shard */
    UPDATE(true),
    /* leader takes off the duty from the shard for any reason may be */
    UNASSIGN(false),
    /* user prompts to delete as a kill state */ 
    DELETE(true),
    /* the duty has finalized */
    FINALIZED(true);
    
    boolean crud;
    DutyEvent(boolean crud) {
        this.crud = crud;
    }
    public boolean isCrud() {
        return crud;
    }
    public boolean is(DutyEvent pe) {
        return this==pe;
    }
}