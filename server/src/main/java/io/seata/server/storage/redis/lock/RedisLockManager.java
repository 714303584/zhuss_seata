/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.server.storage.redis.lock;

import java.util.List;
import java.util.stream.Collectors;

import io.seata.common.executor.Initialize;
import io.seata.common.loader.LoadLevel;
import io.seata.common.util.CollectionUtils;
import io.seata.core.exception.TransactionException;
import io.seata.core.lock.Locker;
import io.seata.server.lock.AbstractLockManager;
import io.seata.server.session.BranchSession;
import io.seata.server.session.GlobalSession;

/**
 * @author funkye
 * redis锁管理
 */
@LoadLevel(name = "redis")
public class RedisLockManager extends AbstractLockManager implements Initialize {

    /**
     * The locker.
     */
    private Locker locker;

    @Override
    public void init() {
        locker = new RedisLocker();
    }

    /**
     * 根据分支事务的session获取锁
     *  直接返回RedisLocker实例
     * @param branchSession the branch session
     * @return
     */
    @Override
    public Locker getLocker(BranchSession branchSession) {
        return locker;
    }

    /**
     * 释放锁
     * @param branchSession
     * @return
     * @throws TransactionException
     */
    @Override
    public boolean releaseLock(BranchSession branchSession) throws TransactionException {
        try {
            //使用Locker进行锁释放
            return getLocker().releaseLock(branchSession.getXid(), branchSession.getBranchId());
        } catch (Exception t) {
            LOGGER.error("unLock error, xid {}, branchId:{}", branchSession.getXid(), branchSession.getBranchId(), t);
            return false;
        }
    }

    /**
     * 释放全局锁
     * @param globalSession the global session
     * @return
     * @throws TransactionException
     */
    @Override
    public boolean releaseGlobalSessionLock(GlobalSession globalSession) throws TransactionException {
        //根据全局会话 -- 获取分支会话
        List<BranchSession> branchSessions = globalSession.getBranchSessions();
        //没有分支会话 释放成功
        if (CollectionUtils.isEmpty(branchSessions)) {
            return true;
        }
        //获取分支ID
        List<Long> branchIds = branchSessions.stream().map(BranchSession::getBranchId).collect(Collectors.toList());
        try {
            //进行锁释放
            return getLocker().releaseLock(globalSession.getXid(), branchIds);
        } catch (Exception t) {
            LOGGER.error("unLock globalSession error, xid:{} branchIds:{}", globalSession.getXid(),
                CollectionUtils.toString(branchIds), t);
            return false;
        }
    }
}