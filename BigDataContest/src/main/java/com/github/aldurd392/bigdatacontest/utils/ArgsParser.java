package com.github.aldurd392.bigdatacontest.utils;


import com.beust.jcommander.Parameter;

public class ArgsParser {

	@Parameter(names = {"-input", "-i"}, description = "Input file path.", 
			arity=1, required=true)
	private String inputFilePath = null;
	
	public String getInputfilePath(){
		return inputFilePath;
	}

	@Parameter(names="-efactor", description = "Euristic Factor: double value.", 
			arity=1, required=true)
	private double efactor = 0;
	
	public double getEuristicFactor(){
		return efactor;
	}
	
	@Parameter(names = "-rho", description = "The fix rho value: double value.", 
			arity=1, required=true)
	private double rho = 0;
	
	public double getRho(){
		return rho;
	}
	
	@Parameter(names = "-iterMode", description = "Iterate ", arity =1)
	private int iterTimes = 0;
	
	public int iterMode(){
		return iterTimes;
	}
	
	@Parameter(names = "-probMode", description = "Probabilistic impl.")
	private boolean probMode = false;
	
	public boolean probMode(){
		return probMode;
	}

}
