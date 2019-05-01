package org.fog.placement;

import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import org.cloudbus.cloudsim.core.CloudSim;
import org.cloudbus.cloudsim.core.SimEntity;
import org.cloudbus.cloudsim.core.SimEvent;
import org.fog.application.AppEdge;
import org.fog.application.AppLoop;
import org.fog.application.AppModule;
import org.fog.application.Application;
import org.fog.entities.Actuator;
import org.fog.entities.FogDevice;
import org.fog.entities.Sensor;
import org.fog.test.perfeval.DCNSFogTB;
import org.fog.utils.Config;
import org.fog.utils.FogEvents;
import org.fog.utils.FogUtils;
import org.fog.utils.NetworkUsageMonitor;
import org.fog.utils.TimeKeeper;

public class Controller extends SimEntity{
	
	public static boolean ONLY_CLOUD = false;
	
	public static int count=0;
	public static double [][]matrix=new double[4][4];
	public static double TotalPower=0;
	public static double TotalCost=0;

	private List<FogDevice> fogDevices;
	private List<Sensor> sensors;
	private List<Actuator> actuators;
	
	private Map<String, Application> applications;
	private Map<String, Integer> appLaunchDelays;

	private Map<String, ModulePlacement> appModulePlacementPolicy;
	
	public Controller(String name, List<FogDevice> fogDevices, List<Sensor> sensors, List<Actuator> actuators) {
		super(name);
		this.applications = new HashMap<String, Application>();
		setAppLaunchDelays(new HashMap<String, Integer>());
		setAppModulePlacementPolicy(new HashMap<String, ModulePlacement>());
		for(FogDevice fogDevice : fogDevices){
			fogDevice.setControllerId(getId());
		}
		setFogDevices(fogDevices);
		setActuators(actuators);
		setSensors(sensors);
		connectWithLatencies();
	}

	private FogDevice getFogDeviceById(int id){
		for(FogDevice fogDevice : getFogDevices()){
			if(id==fogDevice.getId())
				return fogDevice;
		}
		return null;
	}
	
	private void connectWithLatencies(){
		for(FogDevice fogDevice : getFogDevices()){
			FogDevice parent = getFogDeviceById(fogDevice.getParentId());
			if(parent == null)
				continue;
			double latency = fogDevice.getUplinkLatency();
			parent.getChildToLatencyMap().put(fogDevice.getId(), latency);
			parent.getChildrenIds().add(fogDevice.getId());
		}
	}
	
	@Override
	public void startEntity() {
		for(String appId : applications.keySet()){
			if(getAppLaunchDelays().get(appId)==0)
				processAppSubmit(applications.get(appId));
			else
				send(getId(), getAppLaunchDelays().get(appId), FogEvents.APP_SUBMIT, applications.get(appId));
		}

		send(getId(), Config.RESOURCE_MANAGE_INTERVAL, FogEvents.CONTROLLER_RESOURCE_MANAGE);
		
		send(getId(), Config.MAX_SIMULATION_TIME, FogEvents.STOP_SIMULATION);
		
		for(FogDevice dev : getFogDevices())
			sendNow(dev.getId(), FogEvents.RESOURCE_MGMT);

	}
	
	//created by Irfan
	public void storeResults(int offset) {	//offset=0 for cloud and 1 for Fog
		double time=0;
//		System.out.println("inside storeResults()***************************************");
		
		for(Integer loopId : TimeKeeper.getInstance().getLoopIdToTupleIds().keySet()){
			if(getStringForLoopId(loopId)==null) {
				continue;
			}
			else {
				time=time+TimeKeeper.getInstance().getLoopIdToCurrentAverage().get(loopId);		
			}
		}
		
		matrix[0][offset]=time;
		matrix[1][offset]=TotalPower;
		matrix[2][offset]=NetworkUsageMonitor.getNetworkUsage()/Config.MAX_SIMULATION_TIME;	
		matrix[3][offset]=TotalCost;
	
		if(count==1)
			calculateResults();
	}
	
	//created by Irfan
	public void calculateResults() {
		double a,b,c,CloudResult,FogResult;		
		int i=0,j=0;		
		Scanner sc=new Scanner(System.in);
		a=b=c=0.3333;		//default value of weights
		do {
			System.out.println("Enter the weights for alpha (time), beta (energy) and gamma (network consumption)\n");
			a=sc.nextDouble();
			b=sc.nextDouble();
			c=sc.nextDouble();
		}while((a+b+c)!=1);
		
		for(i=0;i<4;i++) {
			j=2;
			while(j<4) {
				matrix[i][j]=matrix[i][j-2]/(matrix[i][0]+matrix[i][1]);
				j++;
			}
		}		//all the percentages have been calculated
		CloudResult=a*matrix[0][2]+b*matrix[1][2]+c*matrix[2][2];
		FogResult=a*matrix[0][3]+b*matrix[1][3]+c*matrix[2][3];
		if(CloudResult<FogResult) {
			System.out.println("Cloud only provides better performance************************************");
		}
		else if(CloudResult>FogResult) {
			System.out.println("Fog nodes provides better performance**********************************");
		}
	}
	
	@Override
	public void processEvent(SimEvent ev) {
		switch(ev.getTag()){
		case FogEvents.APP_SUBMIT:
			processAppSubmit(ev);
			break;
		case FogEvents.TUPLE_FINISHED:
			processTupleFinished(ev);
			break;
		case FogEvents.CONTROLLER_RESOURCE_MANAGE:
			manageResources();
			break;
		case FogEvents.STOP_SIMULATION:
			CloudSim.stopSimulation();
			printTimeDetails();
			printPowerDetails();
			printCostDetails();
			printNetworkUsageDetails();
			
/* commented code is for tawseef project*/			
			if(DCNSFogTB.CLOUD==true)
				storeResults(0);
			else if(DCNSFogTB.CLOUD==false)
				storeResults(1);

			Scanner sc=new Scanner(System.in);
			if(DCNSFogTB.CLOUD==true &&count==0) {
				count++;
//				storeResults(0); 		//store results for cloud
				System.out.println("\n*******************************************************");
				System.out.print("Press \"1\" to execute Cloud-Fog configuration also\t");
				int ch=sc.nextInt();
				if(ch==1) {
					DCNSFogTB.mainIrfan(false);
//					storeResults(1); 		//store results for fog
				}
			}
			else if(DCNSFogTB.CLOUD==false&&count==0) {
				count++;
//				storeResults(1); 			//store results for fog
				System.out.println("\n*******************************************************");
				System.out.print("Press \"1\" to execute Cloud only configuration also\t");
				int ch=sc.nextInt();
				if(ch==1) {
					DCNSFogTB.mainIrfan(true);
//					storeResults(0); 		//store results for cloud
				}	
			}
			sc.close();
			
//			if(DCNSFogTB.CLOUD==false) {
//				//application has been executed with cloud only config once
//				DCNSFogTB.mainIrfan(true);
//			}
			System.out.println("Program execution completed");
			System.exit(0);
			break;
			
		}
	}
	
	private void printNetworkUsageDetails() {
		System.out.println("Total network usage = "+NetworkUsageMonitor.getNetworkUsage()/Config.MAX_SIMULATION_TIME);		
	}

	private FogDevice getCloud(){
		for(FogDevice dev : getFogDevices())
			if(dev.getName().equals("cloud"))
				return dev;
		return null;
	}
	
	private void printCostDetails(){
		//System.out.println("Cost of execution in cloud = "+getCloud().getTotalCost());

		//Added by Irfan
		TotalCost=0;
		for(FogDevice fogDevice : getFogDevices()) {
			TotalCost+=fogDevice.getTotalCost();
			System.out.println("Cost of execution in "+fogDevice.getName()+" = "+fogDevice.getTotalCost());
		}
		System.out.println("*******Total cost of execution is: "+ TotalCost+"\n");
	}
	
	//modified by Irfan - Calculate and display TotalPower also
	private void printPowerDetails() {
		TotalPower=0;
		for(FogDevice fogDevice : getFogDevices()){
			System.out.println(fogDevice.getName() + " : Energy Consumed = "+fogDevice.getEnergyConsumption());
			TotalPower+=fogDevice.getEnergyConsumption();
		}
		System.out.println("*******Total energy consumption is: "+TotalPower+"\n");
	}

	private String getStringForLoopId(int loopId){
		for(String appId : getApplications().keySet()){
			Application app = getApplications().get(appId);
			for(AppLoop loop : app.getLoops()){
				if(loop.getLoopId() == loopId)
					return loop.getModules().toString();
			}
		}
		return null;
	}
	private void printTimeDetails() {
		System.out.println("=========================================");
		System.out.println("============== RESULTS ==================");
		System.out.println("=========================================");
//		System.out.println("EXECUTION TIME : "+ (Calendar.getInstance().getTimeInMillis() - TimeKeeper.getInstance().getSimulationStartTime()));
//		System.out.println("=========================================");
		System.out.println("APPLICATION LOOP DELAYS");
		System.out.println("=========================================");
		for(Integer loopId : TimeKeeper.getInstance().getLoopIdToTupleIds().keySet()){
			/*double average = 0, count = 0;
			for(int tupleId : TimeKeeper.getInstance().getLoopIdToTupleIds().get(loopId)){
				Double startTime = 	TimeKeeper.getInstance().getEmitTimes().get(tupleId);
				Double endTime = 	TimeKeeper.getInstance().getEndTimes().get(tupleId);
				if(startTime == null || endTime == null)
					break;
				average += endTime-startTime;
				count += 1;
			}
			System.out.println(getStringForLoopId(loopId) + " ---> "+(average/count));*/
			if(getStringForLoopId(loopId)==null) {
				continue;
			}
			else {
				System.out.println(getStringForLoopId(loopId) + " ---> "+TimeKeeper.getInstance().getLoopIdToCurrentAverage().get(loopId));			
			}
		}
		System.out.println("=========================================");
		System.out.println("TUPLE CPU EXECUTION DELAY");
		System.out.println("=========================================");
		
		for(String tupleType : TimeKeeper.getInstance().getTupleTypeToAverageCpuTime().keySet()){
			System.out.println(tupleType + " ---> "+TimeKeeper.getInstance().getTupleTypeToAverageCpuTime().get(tupleType));
		}
		
		System.out.println("=========================================");
	}

	protected void manageResources(){
		send(getId(), Config.RESOURCE_MANAGE_INTERVAL, FogEvents.CONTROLLER_RESOURCE_MANAGE);
	}
	
	private void processTupleFinished(SimEvent ev) {
	}
	
	@Override
	public void shutdownEntity() {	
	}
	
	public void submitApplication(Application application, int delay, ModulePlacement modulePlacement){
		FogUtils.appIdToGeoCoverageMap.put(application.getAppId(), application.getGeoCoverage());
		getApplications().put(application.getAppId(), application);
		getAppLaunchDelays().put(application.getAppId(), delay);
		getAppModulePlacementPolicy().put(application.getAppId(), modulePlacement);
		
		for(Sensor sensor : sensors){
			sensor.setApp(getApplications().get(sensor.getAppId()));
		}
		for(Actuator ac : actuators){
			ac.setApp(getApplications().get(ac.getAppId()));
		}
		
		for(AppEdge edge : application.getEdges()){
			if(edge.getEdgeType() == AppEdge.ACTUATOR){
				String moduleName = edge.getSource();
				for(Actuator actuator : getActuators()){
					if(actuator.getActuatorType().equalsIgnoreCase(edge.getDestination()))
						application.getModuleByName(moduleName).subscribeActuator(actuator.getId(), edge.getTupleType());
				}
			}
		}	
	}
	
	public void submitApplication(Application application, ModulePlacement modulePlacement){
		submitApplication(application, 0, modulePlacement);
	}
	
	
	private void processAppSubmit(SimEvent ev){
		Application app = (Application) ev.getData();
		processAppSubmit(app);
	}
	
	private void processAppSubmit(Application application){
		System.out.println(CloudSim.clock()+" Submitted application "+ application.getAppId());
		FogUtils.appIdToGeoCoverageMap.put(application.getAppId(), application.getGeoCoverage());
		getApplications().put(application.getAppId(), application);
		
		ModulePlacement modulePlacement = getAppModulePlacementPolicy().get(application.getAppId());
		for(FogDevice fogDevice : fogDevices){
			sendNow(fogDevice.getId(), FogEvents.ACTIVE_APP_UPDATE, application);
		}
		
		Map<Integer, List<AppModule>> deviceToModuleMap = modulePlacement.getDeviceToModuleMap();
		for(Integer deviceId : deviceToModuleMap.keySet()){
			for(AppModule module : deviceToModuleMap.get(deviceId)){
				sendNow(deviceId, FogEvents.APP_SUBMIT, application);
				sendNow(deviceId, FogEvents.LAUNCH_MODULE, module);
			}
		}
	}

	public List<FogDevice> getFogDevices() {
		return fogDevices;
	}

	public void setFogDevices(List<FogDevice> fogDevices) {
		this.fogDevices = fogDevices;
	}

	public Map<String, Integer> getAppLaunchDelays() {
		return appLaunchDelays;
	}

	public void setAppLaunchDelays(Map<String, Integer> appLaunchDelays) {
		this.appLaunchDelays = appLaunchDelays;
	}

	public Map<String, Application> getApplications() {
		return applications;
	}

	public void setApplications(Map<String, Application> applications) {
		this.applications = applications;
	}

	public List<Sensor> getSensors() {
		return sensors;
	}

	public void setSensors(List<Sensor> sensors) {
		for(Sensor sensor : sensors)
			sensor.setControllerId(getId());
		this.sensors = sensors;
	}

	public List<Actuator> getActuators() {
		return actuators;
	}

	public void setActuators(List<Actuator> actuators) {
		this.actuators = actuators;
	}

	public Map<String, ModulePlacement> getAppModulePlacementPolicy() {
		return appModulePlacementPolicy;
	}

	public void setAppModulePlacementPolicy(Map<String, ModulePlacement> appModulePlacementPolicy) {
		this.appModulePlacementPolicy = appModulePlacementPolicy;
	}
}