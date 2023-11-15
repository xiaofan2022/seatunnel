/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.plus.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigRenderOptions;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.schema.TableSchemaOptions;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;
import org.apache.seatunnel.plus.config.HttpConfig;
import org.apache.seatunnel.plus.config.HttpParameter;
import org.apache.seatunnel.plus.config.JsonField;
import org.apache.seatunnel.plus.config.PageInfo;
import org.apache.seatunnel.plus.config.VerifyInfo;
import org.apache.seatunnel.plus.exception.HttpConnectorException;

import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

@AutoService(SeaTunnelSource.class)
public class HttpPlusSource extends AbstractSingleSplitSource<SeaTunnelRow> {
    protected final HttpParameter httpParameter = new HttpParameter();
    protected PageInfo pageInfo;
    protected SeaTunnelRowType rowType;
    protected JsonField jsonField;
    protected String contentField;
    protected JobContext jobContext;
    protected DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private Config pluginConfig;
    private VerifyInfo verifyInfo;

    @Override
    public String getPluginName() {
        return "HttpPlus";
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    public Boolean verifyCondition(String jsonVerifyExpression) {
        List<String> operators = Lists.newArrayList(">=", "<=", ">", "<", "!=", "=");
        List<String> result =
                operators.stream()
                        .map(
                                t -> {
                                    if (operators.contains(t)) {
                                        return t;
                                    } else {
                                        return null;
                                    }
                                })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        if (result.isEmpty()) {
            return false;
        } else {
            String[] strings = jsonVerifyExpression.split(result.get(0));
        }
        return false;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, HttpConfig.URL.key());
        if (!result.isSuccess()) {
            throw new HttpConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        this.httpParameter.buildWithConfig(pluginConfig);
        buildSchemaWithConfig(pluginConfig);
        buildPagingWithConfig(pluginConfig);
        this.pluginConfig = pluginConfig;
        if (pluginConfig.hasPath("json_verify_expression")) {
            VerifyInfo verifyInfo = new VerifyInfo();
            verifyInfo.setJsonVerifyExpression(pluginConfig.getString("json_verify_expression"));
            verifyInfo.setJsonVerifyValue(pluginConfig.getString("json_verify__value"));
            this.verifyInfo = verifyInfo;
        }
    }

    private void buildPagingWithConfig(Config pluginConfig) {
        if (pluginConfig.hasPath(HttpConfig.PAGEING.key())) {
            pageInfo = new PageInfo();
            Config pageConfig = pluginConfig.getConfig(HttpConfig.PAGEING.key());
            if (pageConfig.hasPath(HttpConfig.TOTAL_PAGE_SIZE.key())) {
                pageInfo.setTotalPageSize(pageConfig.getLong(HttpConfig.TOTAL_PAGE_SIZE.key()));
            }
            if (pageConfig.hasPath(HttpConfig.TOTAL_PAGE_SIZE.key())) {
                pageInfo.setTotalPageSize(pageConfig.getLong(HttpConfig.TOTAL_PAGE_SIZE.key()));
            } else {
                pageInfo.setTotalPageSize(HttpConfig.TOTAL_PAGE_SIZE.defaultValue());
            }

            if (pageConfig.hasPath(HttpConfig.BATCH_SIZE.key())) {
                pageInfo.setBatchSize(pageConfig.getInt(HttpConfig.BATCH_SIZE.key()));
            } else {
                pageInfo.setBatchSize(HttpConfig.BATCH_SIZE.defaultValue());
            }
            if (pageConfig.hasPath(HttpConfig.PAGE_FIELD.key())) {
                pageInfo.setPageField(pageConfig.getString(HttpConfig.PAGE_FIELD.key()));
            }
        }
    }

    protected void buildSchemaWithConfig(Config pluginConfig) {
        if (pluginConfig.hasPath(TableSchemaOptions.SCHEMA.key())) {
            this.rowType = CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();
            // default use json format
            HttpConfig.ResponseFormat format = HttpConfig.FORMAT.defaultValue();
            if (pluginConfig.hasPath(HttpConfig.FORMAT.key())) {
                format =
                        HttpConfig.ResponseFormat.valueOf(
                                pluginConfig
                                        .getString(HttpConfig.FORMAT.key())
                                        .toUpperCase(Locale.ROOT));
            }
            switch (format) {
                case JSON:
                    this.deserializationSchema =
                            new JsonDeserializationSchema(false, false, rowType);
                    if (pluginConfig.hasPath(HttpConfig.JSON_FIELD.key())) {
                        jsonField =
                                getJsonField(pluginConfig.getConfig(HttpConfig.JSON_FIELD.key()));
                    }
                    if (pluginConfig.hasPath(HttpConfig.CONTENT_FIELD.key())) {
                        contentField = pluginConfig.getString(HttpConfig.CONTENT_FIELD.key());
                    }
                    break;
                default:
                    // TODO: use format SPI
                    throw new HttpConnectorException(
                            CommonErrorCode.ILLEGAL_ARGUMENT,
                            String.format(
                                    "Unsupported data format [%s], http connector only support json format now",
                                    format));
            }
        } else {
            this.rowType = CatalogTableUtil.buildSimpleTextSchema();
            this.deserializationSchema = new SimpleTextDeserializationSchema(this.rowType);
        }
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowType;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new HttpPlusSourceReader(
                this.httpParameter,
                readerContext,
                this.deserializationSchema,
                jsonField,
                contentField,
                pageInfo,
                pluginConfig,
                verifyInfo);
    }

    private JsonField getJsonField(Config jsonFieldConf) {
        ConfigRenderOptions options = ConfigRenderOptions.concise();
        return JsonField.builder()
                .fields(JsonUtils.toMap(jsonFieldConf.root().render(options)))
                .build();
    }
}
