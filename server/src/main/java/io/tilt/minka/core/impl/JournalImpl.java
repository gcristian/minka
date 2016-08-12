/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.core.impl;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.Validate;

import io.tilt.minka.core.Journal;
import io.tilt.minka.core.Journal.Fact;
import io.tilt.minka.core.Journal.Story;
import io.tilt.minka.utils.SynchronizedSlidingQueue;

public class JournalImpl implements Journal {
	private final Date creation;
	protected final SynchronizedSlidingQueue<Story<?>> storyQueue;
	protected final Map<Fact, Story<?>> factMap;

	public JournalImpl() {
		this.creation = new Date();
		this.storyQueue = new SynchronizedSlidingQueue<>(100);
		this.factMap = new HashMap<>();
	}
	@Override
	public Date getCreation() {
		return this.creation;
	}
	@Override
	public <T> void commit(Story<T> story) {
		Validate.notNull(story);
		// TODO deserializar la historia y el payload
		// guardar en un archivo, no mantener nada en memoria...
		this.storyQueue.putAndDiscard(story);
	}
	@Override
	public List<Story<?>> readOrdered(Fact fact) {
		return null;
	}
	@Override
	public List<Story<?>> readOrdered() {
		return null;
	}
}