package com.bonree.brfs.authentication;

import com.bonree.brfs.authentication.model.UserModel;
import java.util.List;

/*******************************************************************************
 * 版权信息：博睿宏远科技发展有限公司
 * Copyright: Copyright (c) 2007博睿宏远科技发展有限公司,Inc.All Rights Reserved.
 *
 * @date 2018年3月19日 上午11:26:12
 * @Author: <a href=mailto:weizheng@bonree.com>魏征</a>
 * @Description: 用户操作接口
 ******************************************************************************/
public interface UserOperation {

    public void createUser(UserModel user);

    public void deleteUser(String userName);

    public void updateUser(UserModel user);

    public UserModel getUser(String userName);

    public List<UserModel> getUserList();

}
