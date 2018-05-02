package io.tilt.minka.api.config;

import io.tilt.minka.api.Pallet.Storage;

public class ConsistencyConfiguration {
	protected static final Storage DUTY_STORAGE = Storage.CLIENT_DEFINED;
	private Storage dutyStorage;
	public Storage getDutyStorage() {
		return this.dutyStorage;
	}
	public void setDutyStorage(Storage dutyStorage) {
		this.dutyStorage = dutyStorage;
	}
}