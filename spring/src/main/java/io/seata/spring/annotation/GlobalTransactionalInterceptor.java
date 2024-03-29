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
package io.seata.spring.annotation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.google.common.eventbus.Subscribe;
import io.seata.common.exception.ShouldNeverHappenException;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.common.util.StringUtils;
import io.seata.config.ConfigurationCache;
import io.seata.config.ConfigurationChangeEvent;
import io.seata.config.ConfigurationChangeListener;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.event.EventBus;
import io.seata.core.event.GuavaEventBus;
import io.seata.core.model.GlobalLockConfig;
import io.seata.spring.event.DegradeCheckEvent;
import io.seata.tm.TransactionManagerHolder;
import io.seata.tm.api.DefaultFailureHandlerImpl;
import io.seata.tm.api.FailureHandler;
import io.seata.rm.GlobalLockExecutor;
import io.seata.rm.GlobalLockTemplate;
import io.seata.tm.api.TransactionalExecutor;
import io.seata.tm.api.TransactionalTemplate;
import io.seata.tm.api.transaction.NoRollbackRule;
import io.seata.tm.api.transaction.RollbackRule;
import io.seata.tm.api.transaction.TransactionInfo;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.core.BridgeMethodResolver;
import org.springframework.util.ClassUtils;

import static io.seata.common.DefaultValues.DEFAULT_DISABLE_GLOBAL_TRANSACTION;
import static io.seata.common.DefaultValues.DEFAULT_GLOBAL_TRANSACTION_TIMEOUT;
import static io.seata.common.DefaultValues.DEFAULT_TM_DEGRADE_CHECK;
import static io.seata.common.DefaultValues.DEFAULT_TM_DEGRADE_CHECK_ALLOW_TIMES;
import static io.seata.common.DefaultValues.DEFAULT_TM_DEGRADE_CHECK_PERIOD;

/**
 * The type Global transactional interceptor.
 *
 * @author slievrly
 */
public class GlobalTransactionalInterceptor implements ConfigurationChangeListener, MethodInterceptor {

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalTransactionalInterceptor.class);
    private static final FailureHandler DEFAULT_FAIL_HANDLER = new DefaultFailureHandlerImpl();

    private final TransactionalTemplate transactionalTemplate = new TransactionalTemplate();
    private final GlobalLockTemplate globalLockTemplate = new GlobalLockTemplate();
    private final FailureHandler failureHandler;
    private volatile boolean disable;
    private static int degradeCheckPeriod;
    private static volatile boolean degradeCheck;
    private static int degradeCheckAllowTimes;
    private static volatile Integer degradeNum = 0;
    private static volatile Integer reachNum = 0;
    private static final EventBus EVENT_BUS = new GuavaEventBus("degradeCheckEventBus", true);
    private static ScheduledThreadPoolExecutor executor =
        new ScheduledThreadPoolExecutor(1, new NamedThreadFactory("degradeCheckWorker", 1, true));

    //region DEFAULT_GLOBAL_TRANSACTION_TIMEOUT

    private static int defaultGlobalTransactionTimeout = 0;

    private void initDefaultGlobalTransactionTimeout() {
        LOGGER.info("ifreeshare -- initDefaultGlobalTransactionTimeout timeout value:{}",
                GlobalTransactionalInterceptor.defaultGlobalTransactionTimeout);
        if (GlobalTransactionalInterceptor.defaultGlobalTransactionTimeout <= 0) {
            int defaultGlobalTransactionTimeout;
            try {
                defaultGlobalTransactionTimeout = ConfigurationFactory.getInstance().getInt(
                        ConfigurationKeys.DEFAULT_GLOBAL_TRANSACTION_TIMEOUT, DEFAULT_GLOBAL_TRANSACTION_TIMEOUT);
            } catch (Exception e) {
                LOGGER.error("Illegal global transaction timeout value: " + e.getMessage());
                defaultGlobalTransactionTimeout = DEFAULT_GLOBAL_TRANSACTION_TIMEOUT;
            }
            if (defaultGlobalTransactionTimeout <= 0) {
                LOGGER.warn("Global transaction timeout value '{}' is illegal, and has been reset to the default value '{}'",
                        defaultGlobalTransactionTimeout, DEFAULT_GLOBAL_TRANSACTION_TIMEOUT);
                defaultGlobalTransactionTimeout = DEFAULT_GLOBAL_TRANSACTION_TIMEOUT;
            }
            GlobalTransactionalInterceptor.defaultGlobalTransactionTimeout = defaultGlobalTransactionTimeout;
        }
    }

    //endregion

    /**
     * Instantiates a new Global transactional interceptor.
     * 实例一个新的全局事务拦截器
     *
     * @param failureHandler
     *            the failure handler
     */
    public GlobalTransactionalInterceptor(FailureHandler failureHandler) {
        LOGGER.info("ifreeshare -- FailureHandler:{}",failureHandler.toString());
        this.failureHandler = failureHandler == null ? DEFAULT_FAIL_HANDLER : failureHandler;

        LOGGER.info("ifreeshare -- default_fail_handler:{}",DEFAULT_FAIL_HANDLER.toString());
        this.disable = ConfigurationFactory.getInstance().getBoolean(ConfigurationKeys.DISABLE_GLOBAL_TRANSACTION,
            DEFAULT_DISABLE_GLOBAL_TRANSACTION);
        LOGGER.info("ifreeshare -- DISABLE_GLOBAL_TRANSACTION:{}",ConfigurationKeys.DISABLE_GLOBAL_TRANSACTION);
        degradeCheck = ConfigurationFactory.getInstance().getBoolean(ConfigurationKeys.CLIENT_DEGRADE_CHECK,
            DEFAULT_TM_DEGRADE_CHECK);
        if (degradeCheck) {

            ConfigurationCache.addConfigListener(ConfigurationKeys.CLIENT_DEGRADE_CHECK, this);
            degradeCheckPeriod = ConfigurationFactory.getInstance().getInt(
                ConfigurationKeys.CLIENT_DEGRADE_CHECK_PERIOD, DEFAULT_TM_DEGRADE_CHECK_PERIOD);
            degradeCheckAllowTimes = ConfigurationFactory.getInstance().getInt(
                ConfigurationKeys.CLIENT_DEGRADE_CHECK_ALLOW_TIMES, DEFAULT_TM_DEGRADE_CHECK_ALLOW_TIMES);
            EVENT_BUS.register(this);
            if (degradeCheckPeriod > 0 && degradeCheckAllowTimes > 0) {
                startDegradeCheck();
            }
        }
        this.initDefaultGlobalTransactionTimeout();
    }

    /**
     * 拦截器的执行方法
     * @param methodInvocation
     * @return
     * @throws Throwable
     */
    @Override
    public Object invoke(final MethodInvocation methodInvocation) throws Throwable {
        LOGGER.info("ifreeshare -- invoke:{}",methodInvocation.toString());
        LOGGER.info("ifreeshare -- globalTransaction is begin --");
        Class<?> targetClass =
            methodInvocation.getThis() != null ? AopUtils.getTargetClass(methodInvocation.getThis()) : null;
        // 获取注解方法的实现
        Method specificMethod = ClassUtils.getMostSpecificMethod(methodInvocation.getMethod(), targetClass);
        if (specificMethod != null && !specificMethod.getDeclaringClass().equals(Object.class)) {
            final Method method = BridgeMethodResolver.findBridgedMethod(specificMethod);
            //获取全局事务注解
            final GlobalTransactional globalTransactionalAnnotation =
                getAnnotation(method, targetClass, GlobalTransactional.class);
            LOGGER.info("ifreeshare -- globalTransaction method:{}",method.toGenericString());
            //获取是否有全局锁
            final GlobalLock globalLockAnnotation = getAnnotation(method, targetClass, GlobalLock.class);
            boolean localDisable = disable || (degradeCheck && degradeNum >= degradeCheckAllowTimes);
            if (!localDisable) {
                //全局事务注解不为空
                if (globalTransactionalAnnotation != null) {
                    //执行全局事务
                    return handleGlobalTransaction(methodInvocation, globalTransactionalAnnotation);
                } else if (globalLockAnnotation != null) {
                    //全局锁
                    return handleGlobalLock(methodInvocation, globalLockAnnotation);
                }
            }
        }
        return methodInvocation.proceed();
    }

    Object handleGlobalLock(final MethodInvocation methodInvocation,
        final GlobalLock globalLockAnno) throws Throwable {
        LOGGER.info("ifreeshare -- handleGlobalLock method:{},GlobalLock:{}",
                methodInvocation.toString(),
                globalLockAnno.toString()
                );
        return globalLockTemplate.execute(new GlobalLockExecutor() {
            @Override
            public Object execute() throws Throwable {
                LOGGER.info("ifreeshare -- GlobalLockExecutor.execute() method:{},GlobalLock:{}",
                        methodInvocation.toString(),
                        globalLockAnno.toString()
                );
                return methodInvocation.proceed();
            }

            @Override
            public GlobalLockConfig getGlobalLockConfig() {
                GlobalLockConfig config = new GlobalLockConfig();
                config.setLockRetryInternal(globalLockAnno.lockRetryInternal());
                config.setLockRetryTimes(globalLockAnno.lockRetryTimes());
                return config;
            }
        });
    }

    /**
     * 处理全局事务
     * @param methodInvocation 全局事务方法
     * @param globalTrxAnno
     * @return
     * @throws Throwable
     */
    Object handleGlobalTransaction(final MethodInvocation methodInvocation,
        final GlobalTransactional globalTrxAnno) throws Throwable {
        LOGGER.info("ifreeshare -- GlobalLockExecutor.execute() MethodInvocation:{},GlobalTransactional:{}",
                methodInvocation.toString(),
                globalTrxAnno.toString()
        );
        LOGGER.info("ifreeshare -- handleGlobalTransaction -- start");
        boolean succeed = true;
        try {
            //使用事务模板进行执行
            return transactionalTemplate.execute(new TransactionalExecutor() {
                @Override
                public Object execute() throws Throwable {
                    LOGGER.info("ifreeshare -- MethodInvocation methodInvocation:{}"+methodInvocation.getMethod().getName());
                    return methodInvocation.proceed();
                }
                //事务执行器 -- 事务名称
                public String name() {
                    String name = globalTrxAnno.name();
                    if (!StringUtils.isNullOrEmpty(name)) {
                        return name;
                    }
                    return formatMethod(methodInvocation.getMethod());
                }

                @Override
                public TransactionInfo getTransactionInfo() {
                    // reset the value of timeout
                    //重制超时时间
                    //获取注解的超时时间
                    int timeout = globalTrxAnno.timeoutMills();
                    if (timeout <= 0 || timeout == DEFAULT_GLOBAL_TRANSACTION_TIMEOUT) {
                        timeout = defaultGlobalTransactionTimeout;
                    }
                    //实例化一个事务信息
                    TransactionInfo transactionInfo = new TransactionInfo();
                    //设置超时时间
                    transactionInfo.setTimeOut(timeout);
                    LOGGER.info("ifreeshare -- TransactionInfo.getTransactionInfo -- Transaction name :{}", name());
//                    System.out.println("TransactionInfo name:"+name());
                    //事务名称
                    transactionInfo.setName(name());
                    //事务的传播级别
                    transactionInfo.setPropagation(globalTrxAnno.propagation());
                    //重试次数
                    transactionInfo.setLockRetryInternal(globalTrxAnno.lockRetryInternal());
                    //锁重试次数
                    transactionInfo.setLockRetryTimes(globalTrxAnno.lockRetryTimes());
                    Set<RollbackRule> rollbackRules = new LinkedHashSet<>();
                    for (Class<?> rbRule : globalTrxAnno.rollbackFor()) {
                        //设置回滚规则
                        rollbackRules.add(new RollbackRule(rbRule));
                    }
                    for (String rbRule : globalTrxAnno.rollbackForClassName()) {
                        rollbackRules.add(new RollbackRule(rbRule));
                    }
                    for (Class<?> rbRule : globalTrxAnno.noRollbackFor()) {
                        rollbackRules.add(new NoRollbackRule(rbRule));
                    }
                    for (String rbRule : globalTrxAnno.noRollbackForClassName()) {
                        rollbackRules.add(new NoRollbackRule(rbRule));
                    }
                    transactionInfo.setRollbackRules(rollbackRules);
                    return transactionInfo;
                }
            });
        } catch (TransactionalExecutor.ExecutionException e) {
            TransactionalExecutor.Code code = e.getCode();
            switch (code) {
                case RollbackDone:
                    throw e.getOriginalException();
                case BeginFailure:
                    succeed = false;
                    failureHandler.onBeginFailure(e.getTransaction(), e.getCause());
                    throw e.getCause();
                case CommitFailure:
                    succeed = false;
                    failureHandler.onCommitFailure(e.getTransaction(), e.getCause());
                    throw e.getCause();
                case RollbackFailure:
                    failureHandler.onRollbackFailure(e.getTransaction(), e.getOriginalException());
                    throw e.getOriginalException();
                case RollbackRetrying:
                    failureHandler.onRollbackRetrying(e.getTransaction(), e.getOriginalException());
                    throw e.getOriginalException();
                default:
                    throw new ShouldNeverHappenException(String.format("Unknown TransactionalExecutor.Code: %s", code));
            }
        } finally {
            if (degradeCheck) {
                EVENT_BUS.post(new DegradeCheckEvent(succeed));
            }
            LOGGER.info("ifreeshare -- handleGlobalTransaction -- end");
        }


    }

    public <T extends Annotation> T getAnnotation(Method method, Class<?> targetClass, Class<T> annotationClass) {
        return Optional.ofNullable(method).map(m -> m.getAnnotation(annotationClass))
            .orElse(Optional.ofNullable(targetClass).map(t -> t.getAnnotation(annotationClass)).orElse(null));
    }

    private String formatMethod(Method method) {
        StringBuilder sb = new StringBuilder(method.getName()).append("(");

        Class<?>[] params = method.getParameterTypes();
        int in = 0;
        for (Class<?> clazz : params) {
            sb.append(clazz.getName());
            if (++in < params.length) {
                sb.append(", ");
            }
        }
        return sb.append(")").toString();
    }

    @Override
    public void onChangeEvent(ConfigurationChangeEvent event) {
        LOGGER.info("ifreeshare -- onChangeEvent:dataid-{},changeType-{},newValue-{}",
                event.getDataId(),
                event.getChangeType(),
                event.getNewValue());
        if (ConfigurationKeys.DISABLE_GLOBAL_TRANSACTION.equals(event.getDataId())) {
            LOGGER.info("{} config changed, old value:{}, new value:{}", ConfigurationKeys.DISABLE_GLOBAL_TRANSACTION,
                disable, event.getNewValue());
            disable = Boolean.parseBoolean(event.getNewValue().trim());
        } else if (ConfigurationKeys.CLIENT_DEGRADE_CHECK.equals(event.getDataId())) {
            degradeCheck = Boolean.parseBoolean(event.getNewValue());
            if (!degradeCheck) {
                degradeNum = 0;
            }
        }
    }

    /**
     * auto upgrade service detection
     */
    private static void startDegradeCheck() {
        LOGGER.info("自动升级服务检测");
        LOGGER.info("ifreeshare -- GlobalTransactionalInterceptor.startDegradeCheck()");
        executor.scheduleAtFixedRate(() -> {
            if (degradeCheck) {
                try {
                    String xid = TransactionManagerHolder.get().begin(null, null, "degradeCheck", 60000);
                    TransactionManagerHolder.get().commit(xid);
                    EVENT_BUS.post(new DegradeCheckEvent(true));
                } catch (Exception e) {
                    EVENT_BUS.post(new DegradeCheckEvent(false));
                }
            }
        }, degradeCheckPeriod, degradeCheckPeriod, TimeUnit.MILLISECONDS);
    }

    @Subscribe
    public static void onDegradeCheck(DegradeCheckEvent event) {
        LOGGER.info("ifreeshare -- onDegradeCheck: DegradeCheckEvent:isRequestSuccess-{}",event.isRequestSuccess());
        if (event.isRequestSuccess()) {
            if (degradeNum >= degradeCheckAllowTimes) {
                reachNum++;
                if (reachNum >= degradeCheckAllowTimes) {
                    reachNum = 0;
                    degradeNum = 0;
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("the current global transaction has been restored");
                    }
                }
            } else if (degradeNum != 0) {
                degradeNum = 0;
            }
        } else {
            if (degradeNum < degradeCheckAllowTimes) {
                degradeNum++;
                if (degradeNum >= degradeCheckAllowTimes) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("the current global transaction has been automatically downgraded");
                    }
                }
            } else if (reachNum != 0) {
                reachNum = 0;
            }
        }
    }
}
