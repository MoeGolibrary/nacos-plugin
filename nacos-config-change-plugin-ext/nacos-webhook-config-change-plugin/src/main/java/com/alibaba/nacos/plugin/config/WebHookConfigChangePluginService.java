/*
 * Copyright 1999-2021 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.plugin.config;

import com.alibaba.nacos.api.utils.StringUtils;
import com.alibaba.nacos.common.http.HttpClientBeanHolder;
import com.alibaba.nacos.common.http.HttpRestResult;
import com.alibaba.nacos.common.http.client.NacosRestTemplate;
import com.alibaba.nacos.common.http.param.Header;
import com.alibaba.nacos.common.http.param.Query;
import com.alibaba.nacos.plugin.config.constants.ConfigChangeConstants;
import com.alibaba.nacos.plugin.config.constants.ConfigChangeExecuteTypes;
import com.alibaba.nacos.plugin.config.constants.ConfigChangePointCutTypes;
import com.alibaba.nacos.plugin.config.model.ConfigChangeRequest;
import com.alibaba.nacos.plugin.config.model.ConfigChangeResponse;
import com.alibaba.nacos.plugin.config.spi.ConfigChangePluginService;
import com.slack.api.Slack;
import com.slack.api.methods.response.chat.ChatPostMessageResponse;
import com.slack.api.model.block.HeaderBlock;
import com.slack.api.model.block.SectionBlock;
import com.slack.api.model.block.composition.MarkdownTextObject;
import com.slack.api.model.block.composition.PlainTextObject;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

/**
 * WebHookConfigChangePluginService.
 *
 * @author liyunfei
 **/
public class WebHookConfigChangePluginService implements ConfigChangePluginService {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebHookConfigChangePluginService.class);

    private final NacosRestTemplate restTemplate = HttpClientBeanHolder.getNacosRestTemplate(LOGGER);

    private final Set<Integer> retryResponseCodes = new CopyOnWriteArraySet<Integer>(
            Arrays.asList(HttpStatus.SC_INTERNAL_SERVER_ERROR, HttpStatus.SC_BAD_GATEWAY,
                    HttpStatus.SC_SERVICE_UNAVAILABLE, HttpStatus.SC_GATEWAY_TIMEOUT));

    private static final int INCREASE_STEPS = 1000;

    private static final int DEFAULT_MAX_CONTENT_CAPACITY = 10 * 1024;

    private static final String URL = "url";

    private static final String TYPE = "type";

    private static final String HTTP = "http";

    private static final String SLACK = "slack";

    private static final String TOKEN = "token";

    // channel
    private static final String CHANNEL_ID = "channelID";

    private static final String WEBHOOK_TOKEN = "SLACK_TOKEN";

    private static final String NAMESPACE_IDS = "namespaceIDs";

    @Override
    public void execute(ConfigChangeRequest configChangeRequest, ConfigChangeResponse configChangeResponse) {
        final Properties properties = (Properties) configChangeRequest.getArg(ConfigChangeConstants.PLUGIN_PROPERTIES);

        StringBuilder requestDetails = new StringBuilder();
        requestDetails.append("ConfigChangeRequest [")
                .append("requestType=").append(configChangeRequest.getRequestType().value()).append(", ");
        for (Map.Entry<String, Object> entry : configChangeRequest.getRequestArgs().entrySet()) {
            requestDetails.append(entry.getKey()).append("=").append(entry.getValue()).append(", ");
        }

        StringBuilder responseDetails = new StringBuilder();
        responseDetails.append("ConfigChangeResponse [")
                .append("msg=").append(configChangeResponse.getMsg()).append(", ")
                .append("success=").append(configChangeResponse.isSuccess()).append("]");

        LOGGER.info("WebHookConfigChangePluginService execute,{}, {}, {}", properties, requestDetails, responseDetails);
        ConfigChangeNotifyInfo configChangeNotifyInfo = new ConfigChangeNotifyInfo(
                configChangeRequest.getRequestType().value(), true, (String) configChangeRequest.getArg("modifyTime"));
        wrapConfigChangeNotifyInfo(configChangeNotifyInfo, properties, configChangeRequest, configChangeResponse);

        LOGGER.info("WebHookConfigChangePluginService execute,{}", configChangeNotifyInfo);

        String namespaceIDs = properties.getProperty(NAMESPACE_IDS);
        if (!StringUtils.isBlank(namespaceIDs)) {
            if (Arrays.stream(namespaceIDs.split(",")).noneMatch(namespaceID -> StringUtils.equals(namespaceID, configChangeNotifyInfo.getNamespace()))) {
                LOGGER.info("Ignore namespace {}", configChangeNotifyInfo.getNamespace());
                return;
            }
        }

        ConfigChangePluginExecutor
                .executeAsyncConfigChangePluginTask(new WebhookNotifySingleTask(properties, configChangeNotifyInfo));
    }

    @Override
    public ConfigChangeExecuteTypes executeType() {
        return ConfigChangeExecuteTypes.EXECUTE_AFTER_TYPE;
    }

    @Override
    public String getServiceType() {
        return "webhook";
    }

    @Override
    public int getOrder() {
        return Integer.MAX_VALUE;
    }

    @Override
    public ConfigChangePointCutTypes[] pointcutMethodNames() {
        return ConfigChangePointCutTypes.values();
    }

    private ConfigChangeNotifyInfo wrapConfigChangeNotifyInfo(ConfigChangeNotifyInfo configChangeNotifyInfo,
                                                              Properties properties, ConfigChangeRequest configChangeRequest, ConfigChangeResponse configChangeResponse) {
        final Object contentMaxCapacity = properties.getProperty("contentMaxCapacity");
        final String content = (String) configChangeRequest.getArg("content");
        int maxContent = DEFAULT_MAX_CONTENT_CAPACITY;
        if (contentMaxCapacity != null) {
            maxContent = Integer.parseInt((String) contentMaxCapacity);
        }
        // check content length
        if (content != null) {
            if (content.length() > maxContent) {
                configChangeNotifyInfo.setContent(content.substring(0, maxContent));
            }
        }
        // only diliver err msg so far
        if (configChangeResponse.getMsg() != null) {
            configChangeNotifyInfo.setRs(false);
            configChangeNotifyInfo.setErrorMsg(configChangeResponse.getMsg());
        }
        if (configChangeRequest.getArg("dataId") != null) {
            configChangeNotifyInfo.setDataId((String) configChangeRequest.getArg("dataId"));
        }
        if (configChangeRequest.getArg("group") != null) {
            configChangeNotifyInfo.setGroup((String) configChangeRequest.getArg("group"));
        }
        if (configChangeRequest.getArg("tenant") != null) {
            configChangeNotifyInfo.setTenant((String) configChangeRequest.getArg("tenant"));
        }
        if (configChangeRequest.getArg("namespaceId") != null) {
            configChangeNotifyInfo.setNamespace((String) configChangeRequest.getArg("namespaceId"));
        }
        if (configChangeRequest.getArg("type") != null) {
            configChangeNotifyInfo.setType((String) configChangeRequest.getArg("type"));
        }
        if (configChangeRequest.getArg("tag") != null) {
            configChangeNotifyInfo.setTag((String) configChangeRequest.getArg("tag"));
        }
        if (configChangeRequest.getArg("configTags") != null) {
            configChangeNotifyInfo.setConfigTags((String) configChangeRequest.getArg("configTags"));
        }
        if (configChangeRequest.getArg("appName") != null) {
            configChangeNotifyInfo.setAppName((String) configChangeRequest.getArg("appName"));
        }
        if (configChangeRequest.getArg("use") != null) {
            configChangeNotifyInfo.setUse((String) configChangeRequest.getArg("use"));
        }
        if (configChangeRequest.getArg("srcUser") != null) {
            configChangeNotifyInfo.setSrcUser((String) configChangeRequest.getArg("srcUser"));
        }
        if (configChangeRequest.getArg("srcIp") != null) {
            configChangeNotifyInfo.setSrcIp((String) configChangeRequest.getArg("srcIp"));
        }
        if (configChangeRequest.getArg("effect") != null) {
            configChangeNotifyInfo.setEffect((String) configChangeRequest.getArg("effect"));
        }
        configChangeNotifyInfo.setContent(content);
        return configChangeNotifyInfo;
    }

    private class WebhookNotifySingleTask implements Runnable {
        private Properties properties;

        private ConfigChangeNotifyInfo configChangeNotifyInfo;

        private int retry = 0;

        private final int maxRetry = 6;

        public WebhookNotifySingleTask(Properties properties, ConfigChangeNotifyInfo configChangeNotifyInfo) {
            this.properties = properties;
            this.configChangeNotifyInfo = configChangeNotifyInfo;
        }

        @Override
        public void run() {
            final String type = properties.getProperty(TYPE, HTTP);

            if (type.equals(HTTP)) {
                httpNotify();
            } else if (type.equals(SLACK)) {
                slackNotify();
            } else {
                LOGGER.warn("webhook type {} is not supported", type);
            }
        }

        private void httpNotify() {
            try {
                final String pushUrl = properties.getProperty(URL);
                if (StringUtils.isBlank(pushUrl)) {
                    LOGGER.warn("webhook url is empty,please check it");
                    return;
                }
                HttpRestResult<String> restResult = restTemplate
                        .post(pushUrl, Header.EMPTY, Query.EMPTY, configChangeNotifyInfo, String.class);
                int respCode = restResult.getCode();
                if (respCode != HttpStatus.SC_OK) {
                    if (!retryResponseCodes.contains(respCode)) {
                        LOGGER.warn(
                                "[{}]config change notify request failed,cause request params error,please check it",
                                getClass());
                    } else {
                        LOGGER.warn("config change notify request failed,will retry request {}",
                                restResult.getMessage());
                        retryRequest();
                    }
                }
            } catch (Exception e) {
                if (e instanceof InterruptedIOException || e instanceof UnknownHostException
                        || e instanceof ConnectException || e instanceof SSLException) {
                    LOGGER.warn("config change notify request failed,will retry request({}),cause: {}", retry,
                            e.getMessage());
                    retryRequest();
                } else {
                    LOGGER.warn("config change notify request failed,can not retry,case: {}", e.getMessage());
                }
            }
        }

        /**
         * Slack notify.
         * 先发送一条消息到slack指定channel，包含修改相关的信息
         * 然后再发送一条信息到thread，包含详细的修改信息
         */
        private void slackNotify() {
            try {
                Slack slack = Slack.getInstance();
                String token = properties.getProperty(TOKEN, System.getenv(WEBHOOK_TOKEN));
                String channelID = properties.getProperty(CHANNEL_ID);

                if (StringUtils.isBlank(token) || StringUtils.isBlank(channelID)) {
                    LOGGER.warn("webhook token | channelID is empty,please check it");
                    return;
                }

                String fallbackText = String.format("Config Change Notify: DataId: %s, Group: %s, Namespace: %s",
                        configChangeNotifyInfo.getDataId(),
                        configChangeNotifyInfo.getGroup(),
                        configChangeNotifyInfo.getNamespace());

                ChatPostMessageResponse resp = slack.methods(token).chatPostMessage(req ->
                        req.channel(channelID)
                                .mrkdwn(true)
                                .text(fallbackText) // 添加顶级 text 参数
                                .blocks(Arrays.asList(
                                        HeaderBlock.builder()
                                                .blockId("header-1")
                                                .text(PlainTextObject.builder()
                                                        .text("Config Change Notify")
                                                        .build())
                                                .build(),
                                        SectionBlock.builder()
                                                .blockId("section-1")
                                                .text(MarkdownTextObject.builder()
                                                        .text(fallbackText)
                                                        .build())
                                                .fields(Arrays.asList(
                                                        MarkdownTextObject.builder()
                                                                .text(String.format("*Action:* %s", configChangeNotifyInfo.getAction()))
                                                                .build(),
                                                        MarkdownTextObject.builder()
                                                                .text(String.format("*DataId:* %s", configChangeNotifyInfo.getDataId()))
                                                                .build(),
                                                        MarkdownTextObject.builder()
                                                                .text(String.format("*Group:* %s", configChangeNotifyInfo.getGroup()))
                                                                .build(),
                                                        MarkdownTextObject.builder()
                                                                .text(String.format("*ModifyTime:* %s", configChangeNotifyInfo.getModifyTime()))
                                                                .build(),
                                                        MarkdownTextObject.builder()
                                                                .text(String.format("*ModifyUser:* %s", configChangeNotifyInfo.getSrcUser()))
                                                                .build(),
                                                        MarkdownTextObject.builder()
                                                                .text(String.format("*Namespace:* %s", configChangeNotifyInfo.getNamespace()))
                                                                .build()
                                                ))
                                                .build()
                                ))

                );
                LOGGER.info("slack notify result:{}", resp);
                if (resp.isOk()) {
                    String ts = resp.getMessage().getTs();
                    String threadTs = ts;
                    ChatPostMessageResponse postMessage = slack.methods(token).chatPostMessage(req ->
                            req.channel(channelID)
                                    .threadTs(threadTs)
                                    .mrkdwn(true)
                                    .text(fallbackText) // 添加顶级 text 参数
                                    .blocks(Arrays.asList(
                                            SectionBlock.builder()
                                                    .blockId("section-1")
                                                    .text(MarkdownTextObject.builder()
                                                            .text(String.format("*Content:*\r\n```\r\n%s\r\n```", configChangeNotifyInfo.getContent()))
                                                            .build())
                                                    .fields(Arrays.asList(
                                                                    MarkdownTextObject.builder()
                                                                            .text(String.format("*Type:* %s", configChangeNotifyInfo.getType()))
                                                                            .build(),
                                                                    MarkdownTextObject.builder()
                                                                            .text(String.format("*Desc:* %s", configChangeNotifyInfo.getDesc()))
                                                                            .build(),
                                                                    MarkdownTextObject.builder()
                                                                            .text(String.format("*Effect:* %s", configChangeNotifyInfo.getEffect()))
                                                                            .build(),
                                                                    MarkdownTextObject.builder()
                                                                            .text(String.format("*Use:* %s", configChangeNotifyInfo.getUse()))
                                                                            .build(),
                                                                    MarkdownTextObject.builder()
                                                                            .text(String.format("*AppName:* %s", configChangeNotifyInfo.getAppName()))
                                                                            .build()
                                                            )
                                                    )
                                                    .build()
                                    ))
                    );
                    LOGGER.info("slack notify result:{}", postMessage);
                }

            } catch (Exception e) {
                if (e instanceof InterruptedIOException || e instanceof UnknownHostException
                        || e instanceof ConnectException || e instanceof SSLException) {
                    LOGGER.warn("config change notify request failed,will retry request({}),cause: {}", retry,
                            e.getMessage());
                    retryRequest();
                } else {
                    LOGGER.warn("config change notify request failed,can not retry,case: {}", e.getMessage());
                }
            }

        }

        /**
         * Retry delay time.
         */
        private long getDelay() {
            return (long) retry * retry * INCREASE_STEPS;
        }

        private void retryRequest() {
            retry++;
            if (retry > maxRetry) {
                // Do not retry if over max retry count
                LOGGER.warn("retry to much,give up to push");
                return;
            }
            ConfigChangePluginExecutor.scheduleAsyncConfigChangePluginTask(this, getDelay(), TimeUnit.MILLISECONDS);
        }
    }

}
