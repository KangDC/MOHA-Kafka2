package org.kisti.moha;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.FileSystem;//
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync.CallbackHandler;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MOHA_Manager {
	private static final Logger LOG = LoggerFactory.getLogger(MOHA_Manager.class);
	private YarnConfiguration conf;
	private FileSystem fs;
	private volatile boolean done;
	private int numOfContainers;
	private MOHA_AppInfo appInfo;
	private MOHA_Queue inputQueue;
	private MOHA_Queue outputQueue;
	private AMRMClientAsync<ContainerRequest> amRMClient;
	private NMCallbackHandler containerListener;
	private List<Thread> launchThreads = new ArrayList<>();
	private static String inputQueueName;
	private static String outputQueueName;
	protected NMClientAsync nmClient;
	protected AtomicInteger numCompletedContainers = new AtomicInteger();
	protected AtomicInteger numAllocatedContainers = new AtomicInteger();

	public MOHA_Manager(String[] args) throws IOException {
		// TODO Auto-generated constructor stub
		conf = new YarnConfiguration();
		fs = FileSystem.get(conf);
		for(String str : args){
			LOG.info(str);
		}
		// db = new MOHA_Database();
		appInfo = new MOHA_AppInfo();
		appInfo.setAppId(args[0]);
		appInfo.setExecutorMemory(Integer.parseInt(args[1]));
		appInfo.setNumExecutors(Integer.parseInt(args[2]));
		appInfo.setNumPartitions(appInfo.getNumExecutors());
		appInfo.setJdlPath(args[3]);
		appInfo.setStartingTime(Long.parseLong(args[4]));
		LOG.info("queue name = {}, executor memory = {}, num executors = {}, jdlPath = {}", appInfo.getAppId(),
				appInfo.getExecutorMemory(), appInfo.getNumExecutors(), appInfo.getJdlPath());
		
		inputQueueName = appInfo.getAppId() + MOHA_Properties.inputQueue;
		outputQueueName = "test";// just for testing
		
		String ipAddress = InetAddress.getLocalHost().getHostAddress();
		LOG.info("Host idAdress = {}", ipAddress);
		
		//register the ouputQueue
		outputQueue = new MOHA_Queue(outputQueueName);
		outputQueue.register();
		
		long startManager = System.currentTimeMillis();
		LOG.info(outputQueue.push("put messages to the queue ..."));
		
		//read commands from JDL File
		FileReader fileReader = new FileReader(appInfo.getJdlPath());
		BufferedReader buff = new BufferedReader(fileReader);
		
		//set commands
		appInfo.setNumCommands(Integer.parseInt(buff.readLine()));
		appInfo.setCommand(buff.readLine());
		buff.close();
		
		//register the inputQueue
		inputQueue = new MOHA_Queue(inputQueueName);
		inputQueue.create(appInfo.getNumPartitions(), 1);
		inputQueue.register();
		
		//put messages to the queue
		for (int i = 0; i < appInfo.getNumCommands(); i++) {
			inputQueue.push(i, appInfo.getCommand());
		}
		appInfo.setInitTime(System.currentTimeMillis() - startManager);
		
	}

	private void run() throws YarnException, IOException {
		// TODO Auto-generated method stub
		LOG.info(outputQueue.push("MOHA_Manager is to be running ..."));
		
		//start AMRMClientAsync
		amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, new RMCallbackHandler());
		amRMClient.init(conf);
		LOG.info(outputQueue.push("amRMClient.start() ..."));
		amRMClient.start();
		
		//register AMResponse
		RegisterApplicationMasterResponse response;
		response = amRMClient.registerApplicationMaster(NetUtils.getHostname(), -1, "");
		LOG.info("MOHA Manager is registered with response : {}", response.toString());
		
		//start NMClientAsync
		LOG.info(outputQueue.push("nmClient.start(); ..."));
		containerListener = new NMCallbackHandler(this);
		nmClient = NMClientAsync.createNMClientAsync(containerListener);
		nmClient.init(conf);
		nmClient.start();
		
		appInfo.setAllocationTime(System.currentTimeMillis());
		//request resources to launch containers
		Resource capacity = Records.newRecord(Resource.class);
		capacity.setMemory(appInfo.getExecutorMemory());
		Priority pri = Records.newRecord(Priority.class);
		pri.setPriority(0);
		
		for (int i = 0; i < appInfo.getNumExecutors(); i++) {
			LOG.info(outputQueue.push("Request containers from Resourse Manager, containerNumber = " + i));			
			ContainerRequest containerRequest = new ContainerRequest(capacity, null, null, pri);
			amRMClient.addContainerRequest(containerRequest);
			numOfContainers++;
		}		
		
		while (!done && (numCompletedContainers.get() < numOfContainers)) {

			try {
				// LOG.info(outputQueue.push("The number of completed Containers
				// = " + this.numCompletedContainers.get()));
				Thread.sleep(1000);

			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		LOG.info(outputQueue.push("The number of completed Containers = " + this.numCompletedContainers.get()));		
		LOG.info(outputQueue.push("Containers have all completed, so shutting down NMClient and AMRMClient ..."));

		appInfo.setMakespan(System.currentTimeMillis() - appInfo.getStartingTime());
		//db.appInfoInsert(appInfo);
		nmClient.stop();
		amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "Application complete!", null);
		amRMClient.stop();
		inputQueue.deleteQueue();
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		// start MOHA_Manager
		LOG.info("Starting MOHA Manager...");
		try {
			MOHA_Manager mhm = new MOHA_Manager(args);
			mhm.run();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (YarnException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public class RMCallbackHandler implements CallbackHandler {

		@Override
		public void onContainersCompleted(List<ContainerStatus> statuses) {
			// TODO Auto-generated method stub
			LOG.info("Got response from RM for container ask, completed count = {}", statuses.size());
			for (ContainerStatus status : statuses) {
				numCompletedContainers.incrementAndGet();
				LOG.info("Container completed: ", status.getContainerId());
			}
		}

		@Override
		public void onContainersAllocated(List<Container> containers) {
			// TODO Auto-generated method stub
			LOG.info("Got response from RM for container ask, allocated count = {}", containers.size());
			for (Container container : containers) {
				ContainerLauncher launcher = new ContainerLauncher(container, numAllocatedContainers.getAndIncrement(),
						containerListener);
				Thread mhmThread = new Thread(launcher);
				mhmThread.start();
				launchThreads.add(mhmThread);

			}
		}

		@Override
		public void onShutdownRequest() {
			// TODO Auto-generated method stub
			done = true;
		}

		@Override
		public void onNodesUpdated(List<NodeReport> updatedNodes) {
			// TODO Auto-generated method stub

		}

		@Override
		public float getProgress() {
			// TODO Auto-generated method stub
			float progress = numOfContainers <= 0 ? 0 : (float) numCompletedContainers.get() / numOfContainers;
			return progress;
		}

		@Override
		public void onError(Throwable e) {
			// TODO Auto-generated method stub
			done = true;
			amRMClient.stop();
		}

	}

	protected class ContainerLauncher implements Runnable {
		private Container container;
		private NMCallbackHandler containerListener;
		private int id;

		public ContainerLauncher(Container container, int id, NMCallbackHandler containerListener) {
			super();
			this.container = container;
			this.containerListener = containerListener;
			this.id = id;

			LOG.info(containerListener.toString());
		}

		public String getLaunchCommand(Container container, int id) {
			Vector<CharSequence> vargs = new Vector<>(30);
			vargs.add(Environment.JAVA_HOME.$() + "/bin/java");
			vargs.add(MOHA_TaskExecutor.class.getName());
			vargs.add(appInfo.getAppId());
			vargs.add(container.getId().toString());
			vargs.add(String.valueOf(id));
			vargs.add("1><LOG_DIR>/MOHA_TaskExecutor.stdout");
			vargs.add("2><LOG_DIR>/MOHA_TaskExecutor.stderr");
			StringBuilder command = new StringBuilder();
			for (CharSequence str : vargs) {
				command.append(str).append(" ");
			}
			return command.toString();
		}

		@Override
		public void run() {

			LOG.info("Setting up ContainerLauncher for containerid = {}", container.getId());
			Map<String, LocalResource> localResources = new HashMap<>();
			Map<String, String> env = System.getenv();
			LocalResource appJarFile = Records.newRecord(LocalResource.class);
			appJarFile.setType(LocalResourceType.FILE);
			appJarFile.setVisibility(LocalResourceVisibility.APPLICATION);
			try {
				appJarFile.setResource(ConverterUtils.getYarnUrlFromURI(new URI(env.get("AMJAR"))));
			} catch (URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			appJarFile.setTimestamp(Long.valueOf((env.get("AMJARTIMESTAMP"))));
			appJarFile.setSize(Long.valueOf(env.get("AMJARLEN")));
			localResources.put("app.jar", appJarFile);
			LOG.info("Added {} as a local resource to the Container ", appJarFile.toString());
			ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
			context.setEnvironment(env);
			context.setLocalResources(localResources);

			String command = getLaunchCommand(container, this.id);
			List<String> commands = new ArrayList<>();
			commands.add(command);
			context.setCommands(commands);
			LOG.info("Command to execute MOHA_TaskExecutor = {}", command);
			nmClient.startContainerAsync(container, context);
			LOG.info("Container {} launched!", container.getId());
		}
	}


}
