package org.kisti.moha;

public class MOHA_AppInfo {
	private  String appId;
	private  int executorMemory;
	private  int numExecutors;
	private  String jdlPath;
	private  int numPartitions;
	private  long startingTime;
	private long makespan;
	private  int numCommands;
	private  String command;
	private long initTime;
	private long allocationTime;
	public String getAppId() {
		return appId;
	}
	public void setAppId(String appId) {
		this.appId = appId;
	}
	public int getExecutorMemory() {
		return executorMemory;
	}
	public void setExecutorMemory(int executorMemory) {
		this.executorMemory = executorMemory;
	}
	public int getNumExecutors() {
		return numExecutors;
	}
	public void setNumExecutors(int numExecutors) {
		this.numExecutors = numExecutors;
	}
	public String getJdlPath() {
		return jdlPath;
	}
	public void setJdlPath(String jdlPath) {
		this.jdlPath = jdlPath;
	}
	public int getNumPartitions() {
		return numPartitions;
	}
	public void setNumPartitions(int numPartitions) {
		this.numPartitions = numPartitions;
	}
	public long getStartingTime() {
		return startingTime;
	}
	public void setStartingTime(long startingTime) {
		this.startingTime = startingTime;
	}
	public long getMakespan() {
		return makespan;
	}
	public void setMakespan(long makespan) {
		this.makespan = makespan;
	}
	public int getNumCommands() {
		return numCommands;
	}
	public void setNumCommands(int numCommands) {
		this.numCommands = numCommands;
	}
	public String getCommand() {
		return command;
	}
	public void setCommand(String command) {
		this.command = command;
	}
	public long getInitTime() {
		return initTime;
	}
	public void setInitTime(long initTime) {
		this.initTime = initTime;
	}
	public long getAllocationTime() {
		return allocationTime;
	}
	public void setAllocationTime(long allocationTime) {
		this.allocationTime = allocationTime;
	}
	
}
