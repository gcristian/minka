/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;

import org.apache.commons.lang.Validate;

public class CollectionUtils {

	public static <K, V>V getOrPut(final Map<K, V> map, final K key, final Supplier<V> sup) {
		if (map == null || key == null || sup == null) {
			throw new IllegalArgumentException("null map key or supplier");
		}
		V v = map.get(key);
		if (v == null) {
			map.put(key, v = sup.get());
		}
		return v;
	}
	
	
	public static <T>CircularCollection<T> circular(final Collection<T> collection) {
		return new CircularCollection<>(collection);
	}

	public static <E>SlidingSortedSet<E> sliding(int maxSize) {
		return new SlidingSortedSet<>(maxSize);
	}

	public static class CircularCollection<T> {
		Collection<T> source;
		Iterator<T> it;

		public CircularCollection(final Collection<T> collection) {
			Validate.notEmpty(collection);
			source = collection;
		}

		public T next() {
			if (it == null || !it.hasNext()) {
				it = source.iterator();
				if (!it.hasNext()) {
					return null;
				}
			}
			return it.next();
		}
	}
	
	
	/**
	 * An ordered Set with a limited size polling the tail upon reaching it 
	 */
	public static class SlidingSortedSet<E> {

		private final TreeSet<E> set;
		private final int maxSize;

		private SlidingSortedSet(int maxSize) {
			super();
			Validate.isTrue(maxSize > 0);
			this.maxSize = maxSize;
			this.set = new TreeSet<>();
		}

		public void add(E e) {
			Validate.notNull(e);
			set.add(e);
			if (set.size() >= maxSize) {
				set.pollLast();
			}
		}
		
		public E first() {
		    return this.set.first();
		}

		public List<E> values() {
			return new ArrayList<>(this.set);
		}

	}
	
	/**
	 * A synchronized limited queue where the Head slides to Tail while putting without taking  
	 * @since Nov 29, 2015
	 */
	public static class SynchronizedSlidingQueue<T> extends ArrayBlockingQueue<T> {

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
				return super.take();
			} finally {
				readWrite.writeLock().unlock();
			}
		}
	}

}
