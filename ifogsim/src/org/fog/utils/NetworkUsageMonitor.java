package org.fog.utils;

public class NetworkUsageMonitor {

	public static double networkUsage = 0.0;
	
	public static void sendingTuple(double latency, double tupleNwSize){
		networkUsage += latency*tupleNwSize;
	}
	
	public static double getNetworkUsage(){
		return networkUsage;
	}
}
