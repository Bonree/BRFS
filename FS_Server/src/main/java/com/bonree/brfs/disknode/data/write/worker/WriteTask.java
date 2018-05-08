package com.bonree.brfs.disknode.data.write.worker;

public abstract class WriteTask< Result> implements Runnable {
	protected void onPreExecute() {};
	
	protected abstract Result execute() throws Exception;

	protected abstract void onPostExecute(Result result);
	
	protected abstract void onFailed(Throwable e);
	
	@Override
	public void run() {
		try {
			onPreExecute();
			
			Result result = execute();
			
			onPostExecute(result);
		} catch(Exception e) {
			onFailed(e);
		}
	}
}
