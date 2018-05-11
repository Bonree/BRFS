package com.bonree.brfs;

import java.io.Console;
import java.io.IOException;

import org.quartz.JobExecutionContext;
import org.quartz.UnableToInterruptJobException;

import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;
import com.bonree.brfs.schedulers.task.manager.impl.DefaultReleaseTask;
import com.bonree.brfs.schedulers.task.operation.impl.QuartzOperationStateTask;

public class WatchJob extends QuartzOperationStateTask{

	@Override
	public void caughtException(JobExecutionContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void interrupt() throws UnableToInterruptJobException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void operation(JobExecutionContext context) throws Exception {
		
		MetaTaskManagerInterface release = DefaultReleaseTask.getInstance();
		release.getTaskTypeList();
		
	}
	
	public static void main(String[] args) {
		
			System.out.println("------------------------------------------------------------------before");
//			new ProcessBuilder("cmd", "/c", "cls").inheritIO().start().waitFor();
//			System.out.print("\033[H\033[2J");
//			System.out.flush();
//			Console cons=System.console();
//			System.console().flush();
//			if(cons == null){
//				System.out.println("why");
//			}
			System.out.println("------------------------------------------------------------------after");
		
	}

}
