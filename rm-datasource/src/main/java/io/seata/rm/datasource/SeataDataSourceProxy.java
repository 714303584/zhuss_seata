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
package io.seata.rm.datasource;

import javax.sql.DataSource;

import io.seata.core.model.BranchType;

/**
 * The interface Seata data source.
 * seata的的数据源接口
 * 继承子javax.sql.DataSource接口 提供数据库数据源
 *
 * @author wang.liang
 */
public interface SeataDataSourceProxy extends DataSource {

    /**
     * Gets target data source.
     *
     * @return the target data source
     */
    DataSource getTargetDataSource();

    /**
     * Gets branch type.
     *
     * @return the branch type
     */
    BranchType getBranchType();
}
