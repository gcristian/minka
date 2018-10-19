package io.tilt.minka.core.monitor;

import java.text.SimpleDateFormat;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import io.tilt.minka.domain.CommitTree;
import io.tilt.minka.domain.EntityEvent;
import io.tilt.minka.domain.CommitTree.InsMap;
import io.tilt.minka.domain.CommitTree.LimMap;
import io.tilt.minka.domain.CommitTree.Log;
import io.tilt.minka.domain.CommitTree.Log.StateStamp;

class CommitTreeConverter {

	/** A realistic synthetized view of data structure */
	static JSONObject toJson(final CommitTree ct) {
		final LimMap<Long, InsMap<String , LimMap<EntityEvent, Log>>> tree = ct.getInnerMap();
		JSONObject ret = null;
		if (!tree.isEmpty()) {
			final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
			final JSONObject plans = new JSONObject();
			for (Map.Entry<Long, InsMap<String , LimMap<EntityEvent, Log>>> byPlan: tree.descendingMap().entrySet()) {
				final JSONObject shards = new JSONObject();
				for (Map.Entry<String, LimMap<EntityEvent, Log>> e: byPlan.getValue().entrySet()) {
					final JSONObject events = new JSONObject();
					for (Map.Entry<EntityEvent, Log> ee: e.getValue().entrySet()) {
						final JSONObject stamps = new JSONObject();
						for (StateStamp ss: ee.getValue().getStates()) {
							stamps.put(ss.getState().name().toLowerCase(), sdf.format(ss.getDate()));
						}
						events.put(ee.getKey().name(), stamps);
					}
					shards.put("shard-id:" + e.getKey(), events);
				}
				plans.put("plan-id:" + byPlan.getKey(), shards);
			}
			ret = plans;
		}
		return ret;
	}

	static JSONObject toOrderedJson(final CommitTree ct) {
		final LimMap<Long, InsMap<String , LimMap<EntityEvent, Log>>> tree = ct.getInnerMap();
		JSONObject ret = null;
		if (!tree.isEmpty()) {
			final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
			final JSONArray plans = new JSONArray();
			for (Map.Entry<Long, InsMap<String , LimMap<EntityEvent, Log>>> byPlan: tree.entrySet()) {
				final JSONArray shards = new JSONArray();
				for (Map.Entry<String, LimMap<EntityEvent, Log>> e: byPlan.getValue().entrySet()) {
					final JSONObject events = new JSONObject();
					for (Map.Entry<EntityEvent, Log> ee: e.getValue().entrySet()) {
						final JSONObject stamps = new JSONObject();
						for (StateStamp ss: ee.getValue().getStates()) {
							stamps.put(ss.getState().name().toLowerCase(), sdf.format(ss.getDate()));
						}
						events.put(ee.getKey().name(), stamps);
					}
					JSONObject newshard = new JSONObject();
					newshard.put("shard-id:" + byPlan.getKey(), events);
					shards.put(newshard);
				}
				JSONObject newplan = new JSONObject();
				newplan.put("plan-id:" + byPlan.getKey(), shards);
				
				plans.put(newplan);
			}
			ret = new JSONObject();
			ret.put("plans", plans);
		}
		return ret;
	}
}