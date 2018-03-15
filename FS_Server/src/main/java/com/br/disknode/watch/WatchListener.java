package com.br.disknode.watch;

import java.util.List;

public interface WatchListener {
	void watchHappened(List<Object> metrics);
}
