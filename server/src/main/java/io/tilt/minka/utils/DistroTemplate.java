package io.tilt.minka.utils;

import io.tilt.minka.utils.Journal.Kind;
import io.tilt.minka.utils.Journal.MixinTemplate;

public enum DistroTemplate implements MixinTemplate {
	DC_DANGLING_RESUME		(Kind.MSG, "{}: Registered {} dangling duties {}", 3),
	DC_UNFINISHED_DANGLING	(Kind.MSG,"{}: Previous change's unfinished business saved as Dangling: {}", 2),
	DC_FALSE_UNFINISHED		(Kind.MSG,"{}: Previous change although unfinished hasnt waiting duties", 1),
	DC_SHIPPED_FROM_DUTY	(Kind.MSG," {}: Shipped {} from: {}, Duty: {}", 4),
	;
	
	private final Kind kind;
	private final int size;
	private final String pattern;
	
	DistroTemplate(final Kind kind, final String patt, int size) {
		this.pattern = patt;
		this.kind = kind;
		this.size = size;
	}
	public String getPattern() {
		return null;
	}

	public int getArgSize() {
		return 0;
	}

	public Kind getKind() {
		return null;
	}
	
	@Override
	public String toString() {
		return pattern;
	}
}