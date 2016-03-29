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
package io.tilt.minka.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

/**
 * An ordered Set with a limited size polling the tail upon reaching it 
 */
public class SlidingSortedSet<E> {
    
    private final TreeSet<E> set;
    private final int maxSize;
    
    public SlidingSortedSet(int maxSize) {
        super();
        this.maxSize = maxSize;
        this.set = new TreeSet<>();
    }

    public void add(E e) {
        set.add(e);
        if (set.size()>=maxSize) {
            set.pollLast();
        }
    }

    public List<E> values() {
        return new ArrayList<>(this.set);
    }
        
    
}
