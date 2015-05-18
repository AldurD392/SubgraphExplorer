package com.github.aldurd392.bigdatacontest.utils;


import com.beust.jcommander.Parameter;

import java.util.ArrayList;
import java.util.List;

public class ArgsParser {

    @Parameter(description = "Input file path.", arity = 1)
    private List<String> files = new ArrayList<>();

    @Parameter(names = "-rho", description = "The fix rho value: double value.", arity = 1, required = true)
    private double rho = 2;

    @Parameter(names = "-efactor", description = "Euristic Factor: double value.", arity = 1)
    private double efactor = -1;

    @Parameter(names = "-iterMode", description = "Iterate ", arity = 1)
    private int iterTimes = 0;

    @Parameter(names = "-probMode", description = "Enable probabilistic mode. Each candidate will be chosen with specified probability.")
    private double probMode = 0;

    @Parameter(names = "-outNodes",
            description = "Number of nodes to emits from each SubgraphMapper. This parameter influences the number of rounds.",
            arity = 1
    )
    private int outNodes = 1;


    public String getInputfilePath() {
        return files.get(0);
    }

    public double getEuristicFactor() {
        if (efactor == -1) {  // default case
            efactor = Utils.euristicFactorFunction(rho);
        }

        return efactor;
    }

    public double getRho() {
        return rho;
    }

    public int iterMode() {
        return iterTimes;
    }

    public double probMode() {
        return probMode;
    }

    public int getOutNodes() {
        return outNodes;
    }
}
