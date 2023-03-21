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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import com.google.common.collect.Lists;
import io.seata.common.util.CollectionUtils;
import io.seata.common.util.LambdaUtils;
import io.seata.common.util.StringUtils;
import io.seata.core.lock.AbstractLocker;
import io.seata.core.lock.RowLock;
import io.seata.core.store.LockDO;
import io.seata.server.storage.redis.JedisPooledFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;


import static io.seata.common.Constants.ROW_LOCK_KEY_SPLIT_CHAR;

/**
 * The redis lock store operation
 *redis锁的存储操作
 * @author funkye
 * @author wangzhongxiang
 */
public class RedisLocker extends AbstractLocker {

    private static final Integer SUCCEED = 1;

    private static final Integer FAILED = 0;
    //seata的行级锁
    private static final String DEFAULT_REDIS_SEATA_ROW_LOCK_PREFIX = "SEATA_ROW_LOCK_";

    //seata的全局锁
    private static final String DEFAULT_REDIS_SEATA_GLOBAL_LOCK_PREFIX = "SEATA_GLOBAL_LOCK";

    //全局事务的XID
    private static final String XID = "xid";

    //事务ID
    private static final String TRANSACTION_ID = "transactionId";

    //分支事务ID
    private static final String BRANCH_ID = "branchId";

    //资源ID
    private static final String RESOURCE_ID = "resourceId";

    //锁定的表的名称
    private static final String TABLE_NAME = "tableName";

    //PK ？？ TODO
    private static final String PK = "pk";

    //行健
    private static final String ROW_KEY = "rowKey";

    /**
     * Instantiates a new Redis locker.
     * 实例一个redis的锁
     */
    public RedisLocker() {
    }

    /**
     *获取锁
     * @param rowLocks 行锁
     * @return
     */
    @Override
    public boolean acquireLock(List<RowLock> rowLocks) {
        //行锁
        if (CollectionUtils.isEmpty(rowLocks)) {
            return true;
        }
        //获取XID
        String needLockXid = rowLocks.get(0).getXid();
        //获取分支ID
        Long branchId = rowLocks.get(0).getBranchId();

        //获取jedis实例
        try (Jedis jedis = JedisPooledFactory.getJedisInstance()) {
            List<LockDO> needLockDOS = convertToLockDO(rowLocks);

            if (needLockDOS.size() > 1) {
                needLockDOS = needLockDOS.stream().
                        filter(LambdaUtils.distinctByKey(LockDO::getRowKey))
                        .collect(Collectors.toList());
            }
            //获取锁的健
            List<String> needLockKeys = new ArrayList<>();
            needLockDOS.forEach(lockDO -> needLockKeys.add(buildLockKey(lockDO.getRowKey())));

            //获取redis的pipeline
            Pipeline pipeline1 = jedis.pipelined();
            //获取所有Key的结果
            needLockKeys.stream().forEachOrdered(needLockKey -> pipeline1.hget(needLockKey, XID));
            List<String> existedLockInfos = (List<String>) (List) pipeline1.syncAndReturnAll();
            //
            Map<String, LockDO> needAddLock = new HashMap<>(needLockKeys.size(), 1);

            for (int i = 0; i < needLockKeys.size(); i++) {
                String existedLockXid = existedLockInfos.get(i);
                if (StringUtils.isEmpty(existedLockXid)) {
                    //If empty,we need to lock this row
                    needAddLock.put(needLockKeys.get(i), needLockDOS.get(i));
                } else {
                    if (!StringUtils.equals(existedLockXid, needLockXid)) {
                        //If not equals,means the rowkey is holding by another global transaction
                        return false;
                    }
                }
            }

            if (needAddLock.isEmpty()) {
                return true;
            }
            Pipeline pipeline = jedis.pipelined();
            List<String> readyKeys = new ArrayList<>();
            needAddLock.forEach((key, value) -> {
                //循环加锁
                pipeline.hsetnx(key, XID, value.getXid());
                pipeline.hsetnx(key, TRANSACTION_ID, value.getTransactionId().toString());
                pipeline.hsetnx(key, BRANCH_ID, value.getBranchId().toString());
                pipeline.hset(key, ROW_KEY, value.getRowKey());
                pipeline.hset(key, RESOURCE_ID, value.getResourceId());
                pipeline.hset(key, TABLE_NAME, value.getTableName());
                pipeline.hset(key, PK, value.getPk());
                readyKeys.add(key);
            });
            //获取加锁的结果
            List<Integer> results = (List<Integer>) (List) pipeline.syncAndReturnAll();
            List<List<Integer>> partitions = Lists.partition(results, 7);

            ArrayList<String> success = new ArrayList<>(partitions.size());
            //默认加锁成功
            Integer status = SUCCEED;
            for (int i = 0; i < partitions.size(); i++) {

                if (Objects.equals(partitions.get(i).get(0),FAILED)) {
                    //加锁失败
                    status = FAILED;
                } else {
                    success.add(readyKeys.get(i));
                }
            }

            //If someone has failed,all the lockkey which has been added need to be delete.
            //如果任意一个失败，锁将会失败
            if (FAILED.equals(status)) {
                if (success.size() > 0) {
                    //删除
                    jedis.del(success.toArray(new String[0]));
                }
                //返回获取锁失败
                return false;
            }
            //构建Xid锁的键
            String xidLockKey = buildXidLockKey(needLockXid);
            StringJoiner lockKeysString = new StringJoiner(ROW_LOCK_KEY_SPLIT_CHAR);
            needLockKeys.forEach(lockKeysString::add);
            //设置key
            jedis.hset(xidLockKey, branchId.toString(), lockKeysString.toString());
            //加锁成功
            return true;
        }
    }

    /**
     *  释放锁
     *      删除行锁
     * @param rowLocks
     * @return
     */
    @Override
    public boolean releaseLock(List<RowLock> rowLocks) {
        if (CollectionUtils.isEmpty(rowLocks)) {
            return true;
        }
        //获取基础信息
        String currentXid = rowLocks.get(0).getXid();
        Long branchId = rowLocks.get(0).getBranchId();
        //进行锁转换
        List<LockDO> needReleaseLocks = convertToLockDO(rowLocks);
        String[] needReleaseKeys = new String[needReleaseLocks.size()];
        //构建行锁的Key
        for (int i = 0; i < needReleaseLocks.size(); i ++) {
            needReleaseKeys[i] = buildLockKey(needReleaseLocks.get(i).getRowKey());
        }

        //进行行锁删除
        try (Jedis jedis = JedisPooledFactory.getJedisInstance()) {
            Pipeline pipelined = jedis.pipelined();
            pipelined.del(needReleaseKeys);
            pipelined.hdel(buildXidLockKey(currentXid), branchId.toString());
            pipelined.sync();
            //返回释放成功
            return true;
        }
    }

    /**
     * 释放全局事务的锁
     * @param xid       the xid 全局事务的xid
     * @param branchIds the branch ids 分支事务的ID
     * @return
     */
    @Override
    public boolean releaseLock(String xid, List<Long> branchIds) {
        //分支事务为空 释放锁成功
        if (CollectionUtils.isEmpty(branchIds)) {
            return true;
        }
        //获取redis操作实例
        try (Jedis jedis = JedisPooledFactory.getJedisInstance()) {
            //获取全局事务锁的KEY
            String xidLockKey = buildXidLockKey(xid);
            //获取分支事务的ID
            String[] branchIdsArray = new String[branchIds.size()];
            for (int i = 0; i < branchIds.size(); i++) {
                branchIdsArray[i] = branchIds.get(i).toString();
            }
            //获取行键
            List<String> rowKeys = jedis.hmget(xidLockKey, branchIdsArray);
            //行键不为空
            if (CollectionUtils.isNotEmpty(rowKeys)) {
                //删除行键
                Pipeline pipelined = jedis.pipelined();
                pipelined.hdel(xidLockKey, branchIdsArray);
                rowKeys.forEach(rowKeyStr -> {
                    if (StringUtils.isNotEmpty(rowKeyStr)) {
                        if (rowKeyStr.contains(ROW_LOCK_KEY_SPLIT_CHAR)) {
                            String[] keys = rowKeyStr.split(ROW_LOCK_KEY_SPLIT_CHAR);
                            pipelined.del(keys);
                        } else {
                            pipelined.del(rowKeyStr);
                        }
                    }
                });
                pipelined.sync();
            }
            //释放成功
            return true;
        }
    }

    /**
     * 释放指定分支事务的锁
     * @param xid      the xid
     * @param branchId the branch id
     * @return
     */
    @Override
    public boolean releaseLock(String xid, Long branchId) {
        List<Long> branchIds = new ArrayList<>();
        branchIds.add(branchId);
        return releaseLock(xid, branchIds);
    }

    /**
     *
     * @param rowLocks
     * @return
     */
    @Override
    public boolean isLockable(List<RowLock> rowLocks) {
        if (CollectionUtils.isEmpty(rowLocks)) {
            return true;
        }
        try (Jedis jedis = JedisPooledFactory.getJedisInstance()) {
            List<LockDO> locks = convertToLockDO(rowLocks);
            Set<String> lockKeys = new HashSet<>();
            for (LockDO rowlock : locks) {
                lockKeys.add(buildLockKey(rowlock.getRowKey()));
            }

            String xid = rowLocks.get(0).getXid();
            Pipeline pipeline = jedis.pipelined();
            lockKeys.forEach(key -> pipeline.hget(key, XID));
            List<String> existedXids = (List<String>) (List) pipeline.syncAndReturnAll();
            return existedXids.stream().allMatch(existedXid -> existedXid == null || xid.equals(existedXid));
        }
    }

    /**
     * 构建全局事务锁Key
     * @param xid
     * @return
     */
    private String buildXidLockKey(String xid) {
        return DEFAULT_REDIS_SEATA_GLOBAL_LOCK_PREFIX + xid;
    }

    /**
     * 构建行锁Key
     * @param rowKey
     * @return
     */
    private String buildLockKey(String rowKey) {
        return DEFAULT_REDIS_SEATA_ROW_LOCK_PREFIX + rowKey;
    }

}
