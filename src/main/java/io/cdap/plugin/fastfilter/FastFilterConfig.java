/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.fastfilter;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Config class for {@link FastFilter}.
 */
public class FastFilterConfig extends PluginConfig {
  public static final String SOURCE_FIELD = "sourceField";
  public static final String OPERATOR = "operator";
  public static final String CRITERIA = "criteria";
  public static final String SHOULD_IGNORE_CASE = "shouldIgnoreCase";

  @Name(SOURCE_FIELD)
  @Description("Specifies the input field to use in the filter.")
  private final String sourceField;

  @Name(OPERATOR)
  @Description("The operator to be used for the filter.")
  private final String operator;

  @Name(CRITERIA)
  @Description("The criteria to be used for the filter.")
  @Macro
  private final String criteria;

  @Name(SHOULD_IGNORE_CASE)
  @Description("Set to true to ignore the case of the field when matching.")
  private final Boolean shouldIgnoreCase;

  public FastFilterConfig(String sourceField, String operator, String criteria, boolean shouldIgnoreCase) {
    this.sourceField = sourceField;
    this.operator = operator;
    this.criteria = criteria;
    this.shouldIgnoreCase = shouldIgnoreCase;
  }

  public String getSourceField() {
    return sourceField;
  }

  public Operator getOperator() {
    return Operator.fromToken(operator);
  }

  public String getCriteria() {
    return criteria;
  }

  public Boolean getShouldIgnoreCase() {
    return shouldIgnoreCase;
  }

  public void validate(Schema inputSchema, FailureCollector collector) {
    if (Strings.isNullOrEmpty(sourceField)) {
      collector.addFailure("Source field must be specified.", null).withConfigProperty(SOURCE_FIELD);
    } else if (!inputSchema.getField(sourceField).getSchema().isSimpleOrNullableSimple()) {
      collector.addFailure("Input field must be a simple type but was type: " +
                             (inputSchema.getField(sourceField).getSchema().isNullable() ?
                               inputSchema.getField(sourceField).getSchema().getNonNullable().getType() :
                               inputSchema.getField(sourceField).getSchema().getType()), null)
        .withConfigProperty(SOURCE_FIELD);
    }

    if (!containsMacro(CRITERIA) && Strings.isNullOrEmpty(criteria)) {
      collector.addFailure("Criteria must be specified.", null).withConfigProperty(CRITERIA);
    }

    if (Strings.isNullOrEmpty(operator)) {
      collector.addFailure("Operator must be specified.", null).withConfigProperty(OPERATOR);
    } else {
      try {
        Operator.fromToken(operator);
      } catch (IllegalArgumentException e) {
        collector.addFailure(e.getMessage(), null).withConfigProperty(OPERATOR);
      }

      try {
        if (operator.contains("regex")) {
          Pattern.compile(criteria);
        }
      } catch (PatternSyntaxException e) {
        collector.addFailure("Invalid criteria: " + e.getMessage(), null).withConfigProperty(CRITERIA);
      }
    }
  }

  public enum Operator {
    EQUAL("="),
    NOT_EQUAL("!="),
    GREATER(">"),
    GREATER_OR_EQUAL(">="),
    LESS("<"),
    LESS_OR_EQUAL("<="),
    CONTAINS("contains"),
    NOT_CONTAINS("does not contain"),
    STARTS_WITH("starts with"),
    ENDS_WITH("ends with"),
    NOT_STARTS_WITH("does not start with"),
    NOT_ENDS_WITH("does not end with"),
    MATCH_REGEXP("matches regex"),
    NOT_MATCH_REGEXP("does not match regex");

    private final String token;

    private static final Map<String, Operator> LOOKUP_BY_TOKEN;

    static {
      Map<String, Operator> map = new HashMap<>();
      for (Operator operation : values()) {
        map.put(operation.token, operation);
      }
      LOOKUP_BY_TOKEN = Collections.unmodifiableMap(map);
    }

    Operator(String token) {
      this.token = token;
    }

    public String getToken() {
      return token;
    }

    public static Operator fromToken(String token) {
      Operator operation = LOOKUP_BY_TOKEN.get(token);
      if (operation != null) {
        return operation;
      }
      throw new IllegalArgumentException("Unknown operator type for token: " + token);
    }
  }
}
