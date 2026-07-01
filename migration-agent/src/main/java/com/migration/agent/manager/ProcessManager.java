package com.migration.agent.manager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ProcessManager {
    private static final Logger logger = LoggerFactory.getLogger(ProcessManager.class);
    
    private final String jarPath;
    private final String processName;
    private String taskId;
    private String[] mainArgs;
    private Process process;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private Thread monitorThread;
    
    public ProcessManager(String jarPath, String processName) {
        this.jarPath = jarPath;
        this.processName = processName;
    }
    
    public ProcessManager(String jarPath, String processName, String taskId) {
        this.jarPath = jarPath;
        this.processName = processName;
        this.taskId = taskId;
    }
    
    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }
    
    public void setMainArgs(String[] mainArgs) {
        this.mainArgs = mainArgs;
    }
    
    public void start() throws Exception {
        if (running.get()) {
            logger.warn("{} is already running", processName);
            return;
        }
        
        File jarFile = new File(jarPath);
        if (!jarFile.exists()) {
            throw new RuntimeException("Jar file not found: " + jarPath);
        }
        
        java.util.List<String> command = new java.util.ArrayList<>();
        command.add("java");
        
        if (taskId != null) {
            command.add("-Dtask.id=" + taskId);
            command.add("-Dlogback.configurationFile=files/" + taskId + "/logback.xml");
        }
        
        command.add("-jar");
        command.add(jarPath);
        
        if (mainArgs != null) {
            for (String arg : mainArgs) {
                command.add(arg);
            }
        }
        
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        pb.directory(new File(System.getProperty("user.dir")));
        
        logger.info("Starting {} process: {} with task ID: {}", processName, jarPath, taskId);
        process = pb.start();
        running.set(true);
        
        startMonitorThread();
        logger.info("{} process started with PID: {}", processName, getPid());
    }
    
    private void startMonitorThread() {
        monitorThread = new Thread(() -> {
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            try {
                while ((line = reader.readLine()) != null) {
                    logger.info("[{}] {}", processName, line);
                }
            } catch (Exception e) {
                logger.error("Error reading {} process output", processName, e);
            }
            
            try {
                int exitCode = process.waitFor();
                running.set(false);
                logger.info("{} process exited with code: {}", processName, exitCode);
            } catch (InterruptedException e) {
                logger.error("{} monitor thread interrupted", processName);
            }
        });
        
        monitorThread.setDaemon(true);
        monitorThread.start();
    }
    
    public void stop() {
        if (!running.get()) {
            logger.warn("{} is not running", processName);
            return;
        }
        
        logger.info("Stopping {} process", processName);
        
        if (process != null && process.isAlive()) {
            process.destroy();
            
            try {
                if (!process.waitFor(10, TimeUnit.SECONDS)) {
                    logger.warn("{} process did not exit gracefully, forcing", processName);
                    process.destroyForcibly();
                }
            } catch (InterruptedException e) {
                logger.error("Error stopping {} process", processName, e);
            }
        }
        
        running.set(false);
        
        if (monitorThread != null) {
            monitorThread.interrupt();
        }
        
        logger.info("{} process stopped", processName);
    }
    
    public boolean isRunning() {
        return running.get() && process != null && process.isAlive();
    }
    
    public long getPid() {
        if (process != null) {
            try {
                return process.pid();
            } catch (UnsupportedOperationException e) {
                return -1;
            }
        }
        return -1;
    }
    
    public void ensureRunning() throws Exception {
        if (!isRunning()) {
            logger.warn("{} is not running, restarting...", processName);
            start();
        }
    }
    
    public int waitFor() throws InterruptedException {
        if (process != null) {
            return process.waitFor();
        }
        return -1;
    }
    
    public boolean waitFor(long timeout, TimeUnit unit) throws InterruptedException {
        if (process != null) {
            return process.waitFor(timeout, unit);
        }
        return false;
    }
}