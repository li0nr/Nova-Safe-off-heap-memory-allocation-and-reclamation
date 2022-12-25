package com.nova;

import java.util.Random;

public class RNG {

    private double[] distribution;
    private Random rand;
    
    public RNG(int size) {
        rand = new Random();
        distribution = new double[size];
    }

    public void addNumber(int value, double _distribution) {
    	distribution[value-1] = _distribution*10;
    }

    public int Functions_3() {
    	int coin = rand.nextInt(1000);
        if(coin < distribution[0])
        	return 1;
        else if(coin < distribution[1])
        	return 2;
        else if(coin < distribution[2])
        	return 3;
        return -1;
    }
    
    public int Functions_2() {
    	int coin = rand.nextInt(1000);
        if(coin < distribution[0])
        	return 1;
        else if(coin < distribution[1])
        	return 2;
        return -1;
    }
    
    public static void main(String[] args) {
        RNG drng = new RNG(3);
        drng.addNumber(1, 5);
        drng.addNumber(2, 10);
        drng.addNumber(3, 100);

        int[] hist = new int[3];
        int testCount = 10000;

        HashMap<Integer, Double> test = new HashMap<>();

        for (int i = 0; i < testCount; i++) {
            int random = drng.Functions_3();
            hist[random-1]++;
        }
        
        System.out.println("1 was:"+ hist[0]+"\n");
        System.out.println("2 was:"+ hist[1]+"\n");
        System.out.println("3 was:"+ hist[2]+"\n");

    }

}