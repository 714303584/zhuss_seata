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
package io.seata.spring.tcc;

import java.lang.reflect.Method;
import java.util.Map;

import io.seata.common.Constants;
import io.seata.config.ConfigurationChangeEvent;
import io.seata.config.ConfigurationChangeListener;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.context.RootContext;
import io.seata.core.model.BranchType;
import io.seata.rm.tcc.api.TwoPhaseBusinessAction;
import io.seata.rm.tcc.interceptor.ActionInterceptorHandler;
import io.seata.rm.tcc.remoting.RemotingDesc;
import io.seata.rm.tcc.remoting.parser.DubboUtil;
import io.seata.spring.util.SpringProxyUtils;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import static io.seata.common.DefaultValues.DEFAULT_DISABLE_GLOBAL_TRANSACTION;

/**
 * TCC Interceptor
 * TCC（try-confirm-cancel）事务的拦截器
 * @author zhangsen
 */
public class TccActionInterceptor implements MethodInterceptor, ConfigurationChangeListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(TccActionInterceptor.class);

    private ActionInterceptorHandler actionInterceptorHandler = new ActionInterceptorHandler();

    private volatile boolean disable = ConfigurationFactory.getInstance().getBoolean(
        ConfigurationKeys.DISABLE_GLOBAL_TRANSACTION, DEFAULT_DISABLE_GLOBAL_TRANSACTION);

    /**
     * remoting bean info
     */
    protected RemotingDesc remotingDesc;

    /**
     * Instantiates a new Tcc action interceptor.
     */
    public TccActionInterceptor() {
    }

    /**
     * Instantiates a new Tcc action interceptor.
     *
     * @param remotingDesc the remoting desc
     */
    public TccActionInterceptor(RemotingDesc remotingDesc) {
        this.remotingDesc = remotingDesc;
    }

    /**
     * TCC事务方法的执行
     * @param invocation 需要执行的TCC方法
     * @return
     * @throws Throwable
     */
    @Override
    public Object invoke(final MethodInvocation invocation) throws Throwable {
        // 判定是否在全局事务中
        if (!RootContext.inGlobalTransaction() ||  //不在全局事务中
                disable //  未启用
                || RootContext.inSagaBranch() //本地消息表
        ) {
            //not in transaction
            //不在全局事务中
            return invocation.proceed();
        }
        //获取需要执行的方法
        Method method = getActionInterfaceMethod(invocation);
        //获取两阶段业务
        TwoPhaseBusinessAction businessAction = method.getAnnotation(TwoPhaseBusinessAction.class);
        //try method
        //重试方法
        if (businessAction != null) {
            //save the xid
            String xid = RootContext.getXID();
            //save the previous branchType
            //获取
            BranchType previousBranchType = RootContext.getBranchType();
            //if not TCC, bind TCC branchType
            //如果是TCC 绑定分支事务类型为TCC
            if (BranchType.TCC != previousBranchType) {
                RootContext.bindBranchType(BranchType.TCC);
            }
            try {
                Object[] methodArgs = invocation.getArguments();
                //Handler the TCC Aspect
                // 处理TCC切面
                Map<String, Object> ret = actionInterceptorHandler.proceed(method, methodArgs, xid, businessAction,
                        invocation::proceed);
                //return the final result
                //返回执行结果
                return ret.get(Constants.TCC_METHOD_RESULT);
            }
            finally {
                //if not TCC, unbind branchType
                if (BranchType.TCC != previousBranchType) {
                    RootContext.unbindBranchType();
                }
                //MDC remove branchId
                MDC.remove(RootContext.MDC_KEY_BRANCH_ID);
            }
        }
        return invocation.proceed();
    }

    /**
     * get the method from interface
     * 获取方法
     * @param invocation the invocation
     * @return the action interface method
     */
    protected Method getActionInterfaceMethod(MethodInvocation invocation) {
        Class<?> interfaceType = null;
        try {
            if (remotingDesc == null) {
                interfaceType = getProxyInterface(invocation.getThis());
            } else {
                interfaceType = remotingDesc.getInterfaceClass();
            }
            if (interfaceType == null && remotingDesc.getInterfaceClassName() != null) {
                interfaceType = Class.forName(remotingDesc.getInterfaceClassName(), true,
                    Thread.currentThread().getContextClassLoader());
            }
            if (interfaceType == null) {
                return invocation.getMethod();
            }
            return interfaceType.getMethod(invocation.getMethod().getName(),
                invocation.getMethod().getParameterTypes());
        } catch (NoSuchMethodException e) {
            if (interfaceType != null && !invocation.getMethod().getName().equals("toString")) {
                LOGGER.warn("no such method '{}' from interface {}", invocation.getMethod().getName(), interfaceType.getName());
            }
            return invocation.getMethod();
        } catch (Exception e) {
            LOGGER.warn("get Method from interface failed", e);
            return invocation.getMethod();
        }
    }

    /**
     * get the interface of proxy
     *
     * @param proxyBean the proxy bean
     * @return proxy interface
     * @throws Exception the exception
     */
    protected Class<?> getProxyInterface(Object proxyBean) throws Exception {
        if (DubboUtil.isDubboProxyName(proxyBean.getClass().getName())) {
            //dubbo javaassist proxy
            return DubboUtil.getAssistInterface(proxyBean);
        } else {
            //jdk/cglib proxy
            return SpringProxyUtils.getTargetInterface(proxyBean);
        }
    }

    @Override
    public void onChangeEvent(ConfigurationChangeEvent event) {
        if (ConfigurationKeys.DISABLE_GLOBAL_TRANSACTION.equals(event.getDataId())) {
            LOGGER.info("{} config changed, old value:{}, new value:{}", ConfigurationKeys.DISABLE_GLOBAL_TRANSACTION,
                disable, event.getNewValue());
            disable = Boolean.parseBoolean(event.getNewValue().trim());
        }
    }
}
