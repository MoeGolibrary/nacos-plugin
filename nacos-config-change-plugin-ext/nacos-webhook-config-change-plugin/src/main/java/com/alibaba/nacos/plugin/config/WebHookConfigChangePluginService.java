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
import com.slack.api.model.block.LayoutBlock;
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
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.TimeUnit;

public class WebHookConfigChangePluginService implements ConfigChangePluginService {

    private static final Logger LOGGER = LoggerFactory.getLogger(WebHookConfigChangePluginService.class);

    private final NacosRestTemplate restTemplate = HttpClientBeanHolder.getNacosRestTemplate(LOGGER);

    private final Set<Integer> retryResponseCodes = new CopyOnWriteArraySet<>(
            Arrays.asList(HttpStatus.SC_INTERNAL_SERVER_ERROR, HttpStatus.SC_BAD_GATEWAY,
                    HttpStatus.SC_SERVICE_UNAVAILABLE, HttpStatus.SC_GATEWAY_TIMEOUT));

    private static final int INCREASE_STEPS = 1000;

    private static final int DEFAULT_MAX_CONTENT_CAPACITY = 10 * 1024;

    private static final String URL = "url";
    private static final String TYPE = "type";
    private static final String HTTP = "http";
    private static final String SLACK = "slack";
    private static final String TOKEN = "token";
    private static final String CHANNEL_ID = "channelID";
    private static final String WEBHOOK_TOKEN = "SLACK_TOKEN";
    private static final String NAMESPACE_IDS = "namespaceIDs";

    @Override
    public void execute(ConfigChangeRequest configChangeRequest, ConfigChangeResponse configChangeResponse) {
        Properties properties = (Properties) configChangeRequest.getArg(ConfigChangeConstants.PLUGIN_PROPERTIES);

        StringBuilder requestDetails = new StringBuilder();
        requestDetails.append("ConfigChangeRequest [")
                .append("requestType=").append(configChangeRequest.getRequestType().value()).append(", ")
                .append(formatMap(configChangeRequest.getRequestArgs()));

        StringBuilder responseDetails = new StringBuilder();
        responseDetails.append("ConfigChangeResponse [")
                .append("msg=").append(configChangeResponse.getMsg()).append(", ")
                .append("success=").append(configChangeResponse.isSuccess()).append("]");

        LOGGER.info("WebHookConfigChangePluginService execute,{}, {}, {}", properties, requestDetails, responseDetails);

        ConfigChangeNotifyInfo notifyInfo = wrapConfigChangeNotifyInfo(new ConfigChangeNotifyInfo(
                        configChangeRequest.getRequestType().value(), true, (String) configChangeRequest.getArg("modifyTime")),
                properties, configChangeRequest, configChangeResponse);

        LOGGER.info("WebHookConfigChangePluginService execute,{}", notifyInfo);

        if (!shouldProcessNamespace(properties, notifyInfo)) {
            LOGGER.info("Ignore namespace {}", notifyInfo.getNamespace());
            return;
        }

        ConfigChangePluginExecutor.executeAsyncConfigChangePluginTask(new WebhookNotifySingleTask(properties, notifyInfo));
    }

    private boolean shouldProcessNamespace(Properties properties, ConfigChangeNotifyInfo notifyInfo) {
        String namespaceIDs = properties.getProperty(NAMESPACE_IDS);
        return StringUtils.isBlank(namespaceIDs) || Arrays.stream(namespaceIDs.split(","))
                .anyMatch(namespaceID -> StringUtils.equals(namespaceID, notifyInfo.getNamespace()));
    }

    private String formatMap(Map<String, Object> map) {
        return map.entrySet().stream()
                .map(entry -> entry.getKey() + "=" + entry.getValue())
                .reduce((s1, s2) -> s1 + ", " + s2)
                .orElse("");
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

    private ConfigChangeNotifyInfo wrapConfigChangeNotifyInfo(ConfigChangeNotifyInfo notifyInfo,
                                                              Properties properties, ConfigChangeRequest request, ConfigChangeResponse response) {
        try {
            int maxContent = DEFAULT_MAX_CONTENT_CAPACITY;
            String contentMaxCapacity = properties.getProperty("contentMaxCapacity");
            if (contentMaxCapacity != null && !contentMaxCapacity.isEmpty()) {
                maxContent = Integer.parseInt(contentMaxCapacity);
            }

            String content = (String) request.getArg("content");
            if (content != null && content.length() > maxContent) {
                notifyInfo.setContent(content.substring(0, maxContent));
            } else {
                notifyInfo.setContent(content);
            }

            if (response.getMsg() != null) {
                notifyInfo.setRs(false);
                notifyInfo.setErrorMsg(response.getMsg());
            }

            assignFields(notifyInfo, request);
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid contentMaxCapacity value: {}", properties.getProperty("contentMaxCapacity"), e);
        }
        return notifyInfo;
    }

    private void assignFields(ConfigChangeNotifyInfo notifyInfo, ConfigChangeRequest request) {
        Map<String, Object> args = request.getRequestArgs();
        notifyInfo.setDataId((String) args.get("dataId"));
        notifyInfo.setGroup((String) args.get("group"));
        notifyInfo.setTenant((String) args.get("tenant"));
        notifyInfo.setNamespace((String) args.get("namespace"));
        notifyInfo.setType((String) args.get("type"));
        notifyInfo.setTag((String) args.get("tag"));
        notifyInfo.setConfigTags((String) args.get("configTags"));
        notifyInfo.setAppName((String) args.get("appName"));
        notifyInfo.setUse((String) args.get("use"));
        notifyInfo.setSrcUser((String) args.get("srcUser"));
        notifyInfo.setSrcIp((String) args.get("srcIp"));
        notifyInfo.setEffect((String) args.get("effect"));
    }

    private class WebhookNotifySingleTask implements Runnable {

        private final Properties properties;
        private final ConfigChangeNotifyInfo notifyInfo;
        private final int maxRetry = 6;
        private int retryCount = 0;

        public WebhookNotifySingleTask(Properties properties, ConfigChangeNotifyInfo notifyInfo) {
            this.properties = properties;
            this.notifyInfo = notifyInfo;
        }

        @Override
        public void run() {
            String type = properties.getProperty(TYPE, HTTP);
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
                String pushUrl = properties.getProperty(URL);
                if (StringUtils.isBlank(pushUrl)) {
                    LOGGER.warn("webhook url is empty, please check it");
                    return;
                }

                HttpRestResult<String> result = restTemplate.post(pushUrl, Header.EMPTY, Query.EMPTY, notifyInfo, String.class);
                handleHttpResult(result);
            } catch (Exception e) {
                handleException(e);
            }
        }

        private void handleHttpResult(HttpRestResult<String> result) {
            int respCode = result.getCode();
            if (respCode != HttpStatus.SC_OK) {
                if (retryResponseCodes.contains(respCode)) {
                    retryRequest("HTTP request failed with code " + respCode);
                } else {
                    LOGGER.warn("HTTP request failed with code {}, cannot retry", respCode);
                }
            }
        }

        private void slackNotify() {
            try {
                Slack slack = Slack.getInstance();
                String token = properties.getProperty(TOKEN, System.getenv(WEBHOOK_TOKEN));
                String channelID = properties.getProperty(CHANNEL_ID);

                if (StringUtils.isBlank(token) || StringUtils.isBlank(channelID)) {
                    LOGGER.warn("webhook token or channelID is empty, please check it");
                    return;
                }

                String fallbackText = String.format("Config Change Notify: %s - DataId: %s, Group: %s, Tenant: %s",
                        notifyInfo.getType(), notifyInfo.getDataId(), notifyInfo.getGroup(), notifyInfo.getTenant());

                ChatPostMessageResponse resp = slack.methods(token).chatPostMessage(req ->
                        req.channel(channelID)
                                .mrkdwn(true)
                                .text(fallbackText)
                                .blocks(buildSlackBlocks(fallbackText)));

                if (resp.isOk()) {
                    String threadTs = resp.getMessage().getTs();
                    slack.methods(token).chatPostMessage(req ->
                            req.channel(channelID)
                                    .threadTs(threadTs)
                                    .mrkdwn(true)
                                    .text(fallbackText)
                                    .blocks(Arrays.asList(
                                            SectionBlock.builder()
                                                    .blockId("section-1")
                                                    .text(MarkdownTextObject.builder()
                                                            .text(String.format("```%s\\r\\n%s\\r\\n```", notifyInfo.getType(), notifyInfo.getContent()))
                                                            .build())
                                                    .build()
                                    )));
                }
            } catch (Exception e) {
                handleException(e);
            }
        }

        private List<LayoutBlock> buildSlackBlocks(String fallbackText) {
            return Arrays.asList(
                    HeaderBlock.builder()
                            .blockId("header-1")
                            .text(PlainTextObject.builder().text("Config Change Notify").build())
                            .build(),
                    SectionBlock.builder()
                            .blockId("section-1")
                            .text(MarkdownTextObject.builder().text(fallbackText).build())
                            .fields(Arrays.asList(
                                    MarkdownTextObject.builder().text(String.format("*DataId:* %s", notifyInfo.getDataId())).build(),
                                    MarkdownTextObject.builder().text(String.format("*Group:* %s", notifyInfo.getGroup())).build(),
                                    MarkdownTextObject.builder().text(String.format("*Tenant:* %s", notifyInfo.getTenant())).build(),
                                    MarkdownTextObject.builder().text(String.format("*ModifyTime:* %s", notifyInfo.getModifyTime())).build(),
                                    MarkdownTextObject.builder().text(String.format("*ModifyUser:* %s", notifyInfo.getSrcUser())).build(),
                                    MarkdownTextObject.builder().text(String.format("*Namespace:* %s", notifyInfo.getNamespace())).build()
                            ))
                            .build()
            );
        }

        private void handleException(Exception e) {
            if (e instanceof InterruptedIOException || e instanceof UnknownHostException
                    || e instanceof ConnectException || e instanceof SSLException) {
                retryRequest("Exception occurred: " + e.getMessage());
            } else {
                LOGGER.warn("Request failed, cannot retry: {}", e.getMessage(), e);
            }
        }

        private void retryRequest(String reason) {
            retryCount++;
            if (retryCount <= maxRetry) {
                long delay = (long) Math.pow(retryCount, 2) * INCREASE_STEPS;
                LOGGER.warn("Retrying request (attempt {}/{}) due to: {}", retryCount, maxRetry, reason);
                ConfigChangePluginExecutor.scheduleAsyncConfigChangePluginTask(this, delay, TimeUnit.MILLISECONDS);
            } else {
                LOGGER.warn("Max retry attempts reached, giving up.");
            }
        }
    }
}
