/**
 *  Copyright (c) 2011-2015 Zauber S.A.  -- All rights reserved
 */

package io.tilt.minka.domain;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.Validate;

import io.tilt.minka.api.Duty;

public class DutyDiff {
	
	private static final String DUTY2 = "-duty2";
	private static final String DUTY1 = "-duty1";
	private static final String HASH_CODES = "hash-codes";
	private static final String PALLET_PAYLOAD = "pallet-payload";
	private static final String PAYLOAD = "payload";
	private static final String CLASS_TYPES = "class-types";
	private static final String DIFF = "<!=>";
	private static final String PALLET_ID = "palletId";
	private static final String DUTY = "duty";
	private static final String NULL = "null";
	private static final String NOTNULL = "notnull";
	private static final String DUTY12 = "duty1";
	
	private final Duty<?> duty1;
	private final Duty<?> duty2;
	private Map<String, String> diffs;
	
	public static Map<String, String> diff(final Duty<?> d1, final Duty<?> d2) {
		final DutyDiff dd = new DutyDiff(d1, d2);
		return dd.hasDiffs() ? dd.getDiff() : Collections.emptyMap();
	}
	
	public DutyDiff(final Duty<?> d1, final Duty<?> d2) {
		super();
		Validate.notNull(d2);
		Validate.notNull(d1);
		this.duty1 = d1;
		this.duty2 = d2;
	}
	
	public Map<String, String> getDiff() {
		return diffs;
	}
	
	public boolean hasDiffs() {
		boolean ret = false;
		if (duty1==null || duty2==null) {
			init();
			diffs.put(DUTY1, (duty1 == null ? NULL : NOTNULL));
			diffs.put(DUTY2, (duty2 == null ? NULL : NOTNULL));
			ret = true;
		} else {
			ret |=! hashAndMethod(duty1, duty2, DUTY);
			try {
				ret |=! duty1.getClassType().equals(duty2.getClassType());
				if (!ret && init()) {
					diffs.put(CLASS_TYPES, duty1.getClassType() + DIFF + duty2.getClassType());
				}
			} catch (Exception e) {
				init();
				diffs.put(CLASS_TYPES, "duty1.getClassType().equals() fails:" + e.getMessage());
			}
			ret |=! hashAndMethod(duty1.get(), duty2.get(), PAYLOAD);
			ret |=! duty1.getPalletId().equals(duty2.getPalletId());
			if (!ret && init()) {
				diffs.put(PALLET_ID, duty1.getPalletId() + DIFF + duty2.getPalletId());
			}
			ret |=! hashAndMethod(duty1.get(), duty2.get(), PALLET_PAYLOAD);
		}
		return ret;
	}
	private boolean hashAndMethod(final Object o1 , final Object o2, final String prefix ) {
		boolean ret = true;
		if (o1!=null && o2!=null) {
			ret &= o1.hashCode()==o2.hashCode();
			if (!ret && init()) {
				diffs.put(prefix + HASH_CODES, o1.hashCode() + DIFF + o2.hashCode());
			}
			try {
				ret &= o1.equals(o2);
				if (!ret && init()) {
					diffs.put(prefix, "equals() fails");
				}
			} catch (Exception e) {
				init();
				diffs.put(prefix, "duty1.get().equals() fails:" + e.getMessage());
			}
		} else {
			init();
			diffs.put(prefix + DUTY1, o1==null ? NULL : NOTNULL);
			diffs.put(prefix + DUTY2, o2==null ? NULL : NOTNULL);
		}
		return ret;
	}
	private boolean init() {
		if (diffs==null) {
			diffs = new HashMap<>();
		}
		return true;
	}
}