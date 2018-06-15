package com.github.codegerm.hydra.source;

import org.apache.flume.Context;

public interface PreProcessor {
	
	public void process(Context context) throws Exception;

}
