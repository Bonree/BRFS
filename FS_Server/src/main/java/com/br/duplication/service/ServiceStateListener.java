package com.br.duplication.service;

public interface ServiceStateListener {
	void serviceAdded(Service service);
	void serviceRemoved(Service service);
}
