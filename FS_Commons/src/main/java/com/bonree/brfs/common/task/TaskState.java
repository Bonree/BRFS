package com.bonree.brfs.common.task;

public enum TaskState {
    UNKNOW(0),
    INIT(1),
    RUN(2),
    FINISH(3),
    EXCEPTION(4),
    PAUSE(5),
    RERUN(6),
    FAILED(7);

    private int stat;

    TaskState(int stat) {
        this.stat = stat;
    }

    public int code() {
        return this.stat;
    }

    public static TaskState valueOf(int stat) {
        if (stat == 1) {
            return INIT;
        } else if (stat == 2) {
            return RUN;
        } else if (stat == 3) {
            return FINISH;
        } else if (stat == 4) {
            return EXCEPTION;
        } else if (stat == 5) {
            return PAUSE;
        } else if (stat == 6) {
            return RERUN;
        } else if (stat == 7) {
            return FAILED;
        } else {
            return UNKNOW;
        }
    }
}
