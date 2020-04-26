package com.bonree.brfs.disknode.data.write.worker;

public abstract class WriteTask<T> implements Runnable {
    protected void onPreExecute() {}

    protected abstract T execute() throws Exception;

    protected abstract void onPostExecute(T result);

    protected abstract void onFailed(Throwable e);

    @Override
    public void run() {
        try {
            onPreExecute();

            T result = execute();

            onPostExecute(result);
        } catch (Exception e) {
            onFailed(e);
        }
    }
}
