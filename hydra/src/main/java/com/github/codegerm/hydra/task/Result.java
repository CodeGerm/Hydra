package com.github.codegerm.hydra.task;

import java.util.List;
import java.util.concurrent.Future;

public class Result {
	private String snapshotId;
	private List<Future<Boolean>> futures;
	public String getSnapshotId() {
		return snapshotId;
	}
	public void setSnapshotId(String snapshotId) {
		this.snapshotId = snapshotId;
	}
	public List<Future<Boolean>> getFutures() {
		return futures;
	}
	public void setFutures(List<Future<Boolean>> futures) {
		this.futures = futures;
	}
	public Result(String snapshotId, List<Future<Boolean>> futures) {
		this.snapshotId = snapshotId;
		this.futures = futures;
	}
	@Override
	public String toString() {
		return "Result [snapshotId=" + snapshotId + ", futures=" + futures + "]";
	}
	
	
	
}