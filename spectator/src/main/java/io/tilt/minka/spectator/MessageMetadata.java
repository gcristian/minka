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
package io.tilt.minka.spectator;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Meta containing payload and metadata 
 * @author Cristian Gonzalez
 * @since Oct 14, 2015
 */
public class MessageMetadata implements Serializable {
    
    private static final long serialVersionUID = 1349089355916967771L;
    
    private static final Logger logger = LoggerFactory.getLogger(MessageMetadata.class);
    private static AtomicInteger sequencer;

    private final Object payload;
    private final String inbox;
    private final String originConnectAddress;
    private long createdAt;
    private int sequenceNumber;
    
    static {
        sequencer = new AtomicInteger();
    }
    public MessageMetadata(final Object payload, final String inbox, final String originConnectAddress) {
        this.payload = payload;
        this.originConnectAddress = originConnectAddress;
        this.createdAt = System.currentTimeMillis();
        this.inbox = inbox;
        this.sequenceNumber = sequencer.incrementAndGet();
    }
    
    public Object getPayload() {
        return this.payload;
    }

    public Class<?> getPayloadType() {
        return this.payload.getClass();
    }

    public String getOriginConnectAddress() {
		return originConnectAddress;
	}
    
    @Override
    public String toString() {
        StringBuilder sb= new StringBuilder()
            .append("Msg: SourceIP::SeqID=")
            .append(this.originConnectAddress)
            .append("::")
            .append(this.sequenceNumber)
            .append(", CreatedAt=")
            .append(getCreatedAt())
            .append(", Payload=")
            .append(payload != null ? payload.toString() : "[null]");
            
        return sb.toString(); 
    }

    public long getCreatedAt() {
        return this.createdAt;
    }

    public String getInbox() {
        return this.inbox;
    }
    
}