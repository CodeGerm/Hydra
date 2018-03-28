package com.github.codegerm.hydra.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.Pointer;
import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.WinNT;

public class CommandUtils  {

	private static final Logger logger = LoggerFactory.getLogger(CommandUtils.class);

	private long timeout;

	public CommandUtils(long timeout) {
		this.timeout = timeout;
	}

	
	public void createDir(File dir){
		if(dir.isDirectory())
			return;
		dir.mkdirs();
	}
	
	
	public String executeCmd(String cmd) throws Exception {
		String []content = cmd.split("\\s+");
		List<String> cmdList = Arrays.asList(content);
		return executeCmd(cmdList);
	}

	public String executeCmd(List<String> cmd) throws Exception {
		logger.info("Cmd to be run: " + cmd);
		ProcessBuilder builder = new ProcessBuilder(cmd);
		//builder.redirectErrorStream(true);
		StringBuffer errOutput = new StringBuffer();
		StringBuffer output = new StringBuffer();
		Boolean finished = false;
		try {
			Process process = builder.start();
			if (timeout > 0) {
				finished = process.waitFor(timeout, TimeUnit.MILLISECONDS);
				if (!finished) {
					logger.warn("Cmd timeout after " + timeout + " seconds");
					String pid = Long.toString(getProcessID(process));
					List<String> terminateCmd = generateTerminateCmd(pid);
					ProcessBuilder builder2 = new ProcessBuilder(terminateCmd);
					builder2.start();
					return "Cmd timeout after " + timeout + " seconds";
				} else
					getCmdError(process, errOutput);
				    getCmdOutput(process, output);
			} else {
				process.waitFor();
				getCmdError(process, errOutput);
				getCmdOutput(process, output);
			}
			logger.info("Cmd running result: " + output);
			logger.info("Cmd running error: " + errOutput);
			return errOutput.toString();

		} catch (Exception e) {
			String msg = "Running cmd: [" + cmd + "] failed: ";
			logger.error(msg, e);
			throw e;
			
		}
	}

	private List<String> generateTerminateCmd(String pid) {
		List<String> cmd = new ArrayList<String>();

		cmd.add("taskkill");
		cmd.add("/F");
		cmd.add("/t");
		cmd.add("/PID");
		cmd.add(pid);
		return cmd;
	}

	private static long getProcessID(Process p) {
		long result = -1;
		try {
			// for windows
			if (p.getClass().getName().equals("java.lang.Win32Process")
					|| p.getClass().getName().equals("java.lang.ProcessImpl")) {
				Field f = p.getClass().getDeclaredField("handle");
				f.setAccessible(true);
				long handl = f.getLong(p);
				Kernel32 kernel = Kernel32.INSTANCE;
				WinNT.HANDLE hand = new WinNT.HANDLE();
				hand.setPointer(Pointer.createConstant(handl));
				result = kernel.GetProcessId(hand);
				f.setAccessible(false);
			}
			// for unix based operating systems
			else if (p.getClass().getName().equals("java.lang.UNIXProcess")) {
				Field f = p.getClass().getDeclaredField("pid");
				f.setAccessible(true);
				result = f.getLong(p);
				f.setAccessible(false);
			}
		} catch (Exception ex) {
			result = -1;
		}
		return result;
	}
	
	private void getCmdError(Process process, StringBuffer builder) throws IOException{
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));
			String line = "";
			while ((line = reader.readLine()) != null) {
				builder.append(line + "\n");
			}
		} catch (IOException e){
			throw e;
		} finally {
			if(reader != null)
				reader.close();
		}
	}

	private void getCmdOutput(Process process, StringBuffer builder) throws IOException{
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
			String line = "";
			while ((line = reader.readLine()) != null) {
				builder.append(line + "\n");
			}
		} catch (IOException e){
			throw e;
		} finally {
			if(reader != null)
				reader.close();
		}
	}

}

