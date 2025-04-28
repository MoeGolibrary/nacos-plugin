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

import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.plugin.config.constants.ConfigChangeExecuteTypes;
import com.alibaba.nacos.plugin.config.constants.ConfigChangePointCutTypes;
import com.alibaba.nacos.plugin.config.model.ConfigChangeRequest;
import com.alibaba.nacos.plugin.config.model.ConfigChangeResponse;
import com.alibaba.nacos.plugin.config.spi.ConfigChangePluginService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.yaml.snakeyaml.Yaml;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Properties;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * FileFormatConfigChangePluginService.
 *
 * @author liyunfei
 **/
public class FileFormatConfigChangePluginService implements ConfigChangePluginService {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileFormatConfigChangePluginService.class);

    /**
     * the relationship of type and function of validating the file.
     */
    private static final ConcurrentHashMap<String, Function<String, Boolean>> fileValidateMap = new ConcurrentHashMap<>(6);

    @Override
    public void execute(ConfigChangeRequest configChangeRequest, ConfigChangeResponse configChangeResponse) {
        try {
            // RPC- dont need to validate
            if (configChangeRequest.getRequestType().equals(ConfigChangePointCutTypes.PUBLISH_BY_RPC)) {
                return;
            }
            // remove
            if (
                    configChangeRequest.getRequestType().equals(ConfigChangePointCutTypes.REMOVE_BY_HTTP) ||
                            configChangeRequest.getRequestType().equals(ConfigChangePointCutTypes.REMOVE_BY_RPC) ||
                            configChangeRequest.getRequestType().equals(ConfigChangePointCutTypes.REMOVE_BATCH_HTTP)
            ) {
                configChangeResponse.setSuccess(true);
                return;
            }

            // according to pjp acquire content and type
            Object contentObj = configChangeRequest.getArg("content");
            Object typeObj = configChangeRequest.getArg("type");

            if (!(contentObj instanceof String) || !(typeObj instanceof String)) {
                LOGGER.warn("Invalid content or type: content={}, type={}", contentObj, typeObj);
                configChangeResponse.setSuccess(false);
                configChangeResponse.setMsg("Invalid content or type");
                return;
            }

            String content = (String) contentObj;
            String type = (String) typeObj;

            if (StringUtils.isEmpty(content) || StringUtils.isEmpty(type)) {
                LOGGER.warn("Content or type is empty: content={}, type={}", content, type);
                configChangeResponse.setSuccess(false);
                configChangeResponse.setMsg("Content or type is empty");
                return;
            }

            boolean isValidate = validate(content, type);
            if (!isValidate) {
                LOGGER.warn("Content of publish content is not consistent with type: content={}, type={}", content, type);
                configChangeResponse.setSuccess(false);
                configChangeResponse.setMsg("Content of publish content is not consistent with type");
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred during validation", e);
            configChangeResponse.setSuccess(false);
            configChangeResponse.setMsg("Internal server error");
        }
    }

    @Override
    public ConfigChangeExecuteTypes executeType() {
        return ConfigChangeExecuteTypes.EXECUTE_BEFORE_TYPE;
    }

    @Override
    public String getServiceType() {
        return "fileformatcheck";
    }

    @Override
    public int getOrder() {
        return 0;
    }

    @Override
    public ConfigChangePointCutTypes[] pointcutMethodNames() {
        return new ConfigChangePointCutTypes[]{ConfigChangePointCutTypes.PUBLISH_BY_HTTP,
                ConfigChangePointCutTypes.PUBLISH_BY_RPC};
    }

    static {
        loadUtils();
    }

    static void loadUtils() {
        fileValidateMap.put("text", textValidate());
        fileValidateMap.put("json", jsonValidate());
        fileValidateMap.put("xml", xmlValidate());
        fileValidateMap.put("html", htmlValidate());
        fileValidateMap.put("properties", propertiesValidate());
        fileValidateMap.put("yaml", yamlValidate());
    }

    /**
     * Validate file is consistent with type.
     *
     * @param content string content.
     * @param type    file type.
     * @return
     */
    public static boolean validate(String content, String type) {
        Function<String, Boolean> function = fileValidateMap.getOrDefault(type, defaultValidate());
        if (function == null) {
            LOGGER.warn("Unsupported file format: {}", type);
            return false;
        }
        return function.apply(content);
    }

    /**
     * Default validate function for unsupported types.
     */
    static Function<String, Boolean> defaultValidate() {
        return content -> {
            LOGGER.warn("Default validation applied for unsupported type");
            return false;
        };
    }

    /**
     * Validate text format.
     */
    static Function<String, Boolean> textValidate() {
        return Objects::nonNull;
    }

    /**
     * Validate json format using Jackson for better performance and stability.
     */
    static Function<String, Boolean> jsonValidate() {
        return content -> {
            try {
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                mapper.readTree(content);
                return true;
            } catch (Exception e) {
                return false;
            }
        };
    }

    /**
     * Validate xml format with thread-safe DocumentBuilder instance.
     */
    static Function<String, Boolean> xmlValidate() {
        return content -> {
            try {
                DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
                documentBuilderFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
                DocumentBuilder builder = documentBuilderFactory.newDocumentBuilder();
                builder.parse(new InputSource(new StringReader(content)));
                return true;
            } catch (Exception e) {
                return false;
            }
        };
    }

    /**
     * Validate html format.
     */
    static Function<String, Boolean> htmlValidate() {
        String regex = "<([^>]*)>";
        Pattern pattern = Pattern.compile(regex);
        return content -> {
            Matcher matcher = pattern.matcher(content);
            return matcher.find();
        };
    }

    /**
     * Validate properties format.
     */
    static Function<String, Boolean> propertiesValidate() {
        return content -> {
            try {
                Properties properties = new Properties();
                properties.load(new StringReader(content));
                return true;
            } catch (Exception e) {
                return false;
            }
        };
    }

    /**
     * Validate yaml format.
     */
    static Function<String, Boolean> yamlValidate() {
        return content -> {
            Yaml yaml = new Yaml();
            try {
                Object o = yaml.loadAs(content, Object.class);
                return o instanceof LinkedHashMap;
            } catch (Exception e) {
                return false;
            }
        };
    }
}
