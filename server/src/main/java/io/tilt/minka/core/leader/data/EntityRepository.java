package io.tilt.minka.core.leader.data;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import io.tilt.minka.api.Duty;
import io.tilt.minka.domain.ShardEntity;

public class EntityRepository {

	final Map<ShardEntity, File> map = new HashMap<>();
	
	public InputStream read(final ShardEntity entity) {
		try {
			final java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
			ObjectOutputStream oos;
			oos = new ObjectOutputStream(baos);
			oos.writeObject("holitas");;
			final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
			return bais;
		} catch (Exception e) {
			return null;
		}
	}
	
	public void save(final Collection<Duty.LoadedDuty> entity) {
		
	}
	public void save(final Duty.LoadedDuty entity) {
		// save
	}
}
