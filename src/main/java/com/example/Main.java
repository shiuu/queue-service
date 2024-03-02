package com.example;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Main {
    public static Logger logger = LogManager.getLogger(Main.class);
    
    public static void main(String[] args) {
        logger.trace("We've just greeted the user!");
        logger.info("Hello");
        logger.debug("Hello");
        System.out.println("Hello");
    }
}
