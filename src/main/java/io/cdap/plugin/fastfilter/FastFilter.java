/*
 * Copyright © 2019 CDAP
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

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;

import java.util.regex.Pattern;

/**
 * Quickly filters records based on a set criteria.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("FastFilter")
@Description("Only allows records through that pass the specified criteria.")
public final class FastFilter extends Transform<StructuredRecord, StructuredRecord> {
  private final Config config;
  private Pattern regexPattern;

  @VisibleForTesting
  public FastFilter(Config config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    config.validate(inputSchema);
    // Just checking that the operator is valid
    shouldAllowThrough("", config.operator, "");
    pipelineConfigurer.getStageConfigurer().setOutputSchema(pipelineConfigurer.getStageConfigurer().getInputSchema());
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    if (config.operator.contains("regex")) {
      regexPattern = Pattern.compile(config.criteria);
    }
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    if (in.get(config.sourceField) != null) {
      Schema inputSchema = in.getSchema();
      if (!inputSchema.getField(config.sourceField).getSchema().isSimpleOrNullableSimple()) {
        throw new IllegalArgumentException("Input field must be a simple type but was type: " +
                                             inputSchema.getField(config.sourceField).getSchema().getType());
      }
      String sourceContent = String.valueOf(in.get(config.sourceField)).trim();
      if (config.shouldIgnoreCase) {
        sourceContent = sourceContent.toLowerCase();
      }
      if (shouldAllowThrough(sourceContent, config.operator, config.criteria)) {
        emitter.emit(in);
      }
    }
  }

  private boolean shouldAllowThrough(String sourceContent, String operator, String criteria) {
    switch (operator) {
      case "=":
        return sourceContent.equals(criteria);
      case "!=":
        return !sourceContent.equals(criteria);
      case ">":
        return (sourceContent.compareTo(criteria) > 0);
      case ">=":
        return (sourceContent.compareTo(criteria) >= 0);
      case "<":
        return (sourceContent.compareTo(criteria) < 0);
      case "<=":
        return (sourceContent.compareTo(criteria) <= 0);
      case "contains":
        return sourceContent.contains(criteria);
      case "does not contain":
        return !sourceContent.contains(criteria);
      case "starts with":
        return sourceContent.startsWith(criteria);
      case "ends with":
        return sourceContent.endsWith(criteria);
      case "does not start with":
        return !sourceContent.startsWith(criteria);
      case "does not end with":
        return !sourceContent.endsWith(criteria);
      case "matches regex":
        return regexPattern.matcher(sourceContent).find();
      case "does not match regex":
        return !regexPattern.matcher(sourceContent).find();
      default:
        throw new IllegalArgumentException("Invalid operator: " + operator);
    }
  }

  /**
   * Fast filter plugin configuration.
   */
  public static class Config extends PluginConfig {
    @Name("sourceField")
    @Description("Specifies the input field to use in the filter.")
    private final String sourceField;

    @Name("operator")
    @Description("The operator to be used for the filter.")
    private final String operator;

    @Name("criteria")
    @Description("The criteria to be used for the filter.")
    @Macro
    private final String criteria;

    @Name("shouldIgnoreCase")
    @Description("Set to true to ignore the case of the field when matching.")
    private final Boolean shouldIgnoreCase;


    public Config(String sourceField, String operator, String criteria, boolean shouldIgnoreCase) {
      this.sourceField = sourceField;
      this.operator = operator;
      this.criteria = criteria;
      this.shouldIgnoreCase = shouldIgnoreCase;
    }

    public void validate(Schema inputSchema) {
      if (operator.contains("regex")) {
        Pattern.compile(criteria);
      }
      if (!inputSchema.getField(sourceField).getSchema().isSimpleOrNullableSimple()) {
        throw new IllegalArgumentException("Input field must be a simple type but was type: " +
                                             inputSchema.getField(sourceField).getSchema().getType());
      }
    }
  }
}
