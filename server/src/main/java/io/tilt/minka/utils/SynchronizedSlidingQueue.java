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
package io.tilt.minka.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A synchronized limited queue where the Head slides to Tail while putting without taking  
 * 
 * @author Cristian Gonzalez
 * @since Nov 29, 2015
 */
public class SynchronizedSlidingQueue<T> extends ArrayBlockingQueue<T> {

		private static final long serialVersionUID = 1L;
		private final ReadWriteLock readWrite;

		public SynchronizedSlidingQueue(int size) {
			super(size, true);
			this.readWrite = new ReentrantReadWriteLock(true);
		}

		public boolean isSliding() {
			return remainingCapacity() == 0;
		}

		public List<T> getAllImmutable() {
			List<T> list = new ArrayList<>();
			readWrite.readLock().lock();
			try {
				Iterator<T> it = iterator();
				while (it.hasNext()) {
						list.add(it.next());
				}
			} finally {
				readWrite.readLock().unlock();
				;
			}
			return list;
		}

		public void putAndDiscard(T t) {
			putAndTake(t);
		}

		public T putAndTake(T t) {
			T discarded = null;
			readWrite.writeLock().lock();
			try {
				if (remainingCapacity() == 0) {
						discarded = poll();
				}
				add(t);
			} finally {
				readWrite.writeLock().unlock();
			}
			return discarded;
		}

		public T taste() {
			return peek();
		}

		public T take() throws InterruptedException {
			try {
				readWrite.writeLock().lock();
				return take();
			} finally {
				readWrite.writeLock().unlock();
			}
		}
}