package com.bonree.brfs.schedulers.exception;

public class ParamsErrorException extends Exception {
	private static final long serialVersionUID = 1l;
	public ParamsErrorException(String message) {
		super(message);
	}
	public ParamsErrorException(){
		super();
	}
}
