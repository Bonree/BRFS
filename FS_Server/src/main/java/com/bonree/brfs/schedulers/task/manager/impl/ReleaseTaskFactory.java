package com.bonree.brfs.schedulers.task.manager.impl;


import com.bonree.brfs.schedulers.task.manager.MetaTaskManagerInterface;

public class ReleaseTaskFactory {
	public static MetaTaskManagerInterface getInstance(int type){
		DefaultReleaseTask tmp = DefaultReleaseTask.getInstance();
		return tmp;
	}
}
