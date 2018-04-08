package com.bonree.brfs.rebalance;

public class Constants {

    public final static String zkUrl = "192.168.101.86:2181";

    public final static String BASE_PATH = "/brfs/wz/rebalance";

    public final static String SEPARATOR = "/";

    public final static String CHANGE_NODE = "changes";

    public final static String LEADER_NODE = "leadermonitor";

    public final static String TASKS_NODE = "tasks";

    public final static String TASK_NODE = "task";

    public final static String ROLES_NODE = "roles";

    public final static String ROLE_NODE = "role";

    public final static String PATH_CHANGES = BASE_PATH + SEPARATOR + CHANGE_NODE;

    public final static String PATH_LEADER = BASE_PATH + SEPARATOR + LEADER_NODE;

    public final static String PATH_TASKS = BASE_PATH + SEPARATOR + TASKS_NODE;

    public final static String PATH_ROLES = BASE_PATH + SEPARATOR + ROLES_NODE;

}
