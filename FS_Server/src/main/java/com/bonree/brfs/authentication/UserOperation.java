package com.bonree.brfs.authentication;

import com.bonree.brfs.authentication.model.UserModel;

public interface UserOperation {

    public void createUser(UserModel user);

    public void deleteUser(UserModel user);

    public void updateUser(UserModel user);

    public UserModel getUser(String userName);

}
