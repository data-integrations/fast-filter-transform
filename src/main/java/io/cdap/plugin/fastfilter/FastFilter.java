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
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.StageConfigurer;
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
  private final FastFilterConfig config;
  private Pattern regexPattern;
  private FastFilterConfig.Operator operator;

  @VisibleForTesting
  public FastFilter(FastFilterConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);
    StageConfigurer stageConfigurer = pipelineConfigurer.getStageConfigurer();
    FailureCollector collector = stageConfigurer.getFailureCollector();
    Schema inputSchema = stageConfigurer.getInputSchema();

    config.validate(inputSchema, collector);
    collector.getOrThrowException();

    pipelineConfigurer.getStageConfigurer().setOutputSchema(pipelineConfigurer.getStageConfigurer().getInputSchema());
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    operator = config.getOperator();
    if (operator.getToken().contains("regex")) {
      regexPattern = Pattern.compile(config.getCriteria());
    }
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    if (in.get(config.getSourceField()) != null) {
      Schema inputSchema = in.getSchema();
      if (!inputSchema.getField(config.getSourceField()).getSchema().isSimpleOrNullableSimple()) {
        throw new IllegalArgumentException("Input field must be a simple type but was type: " +
                                             inputSchema.getField(config.getSourceField()).getSchema().getType());
      }
      String sourceContent = String.valueOf(in.<Object>get(config.getSourceField())).trim();
      if (config.getShouldIgnoreCase()) {
        sourceContent = sourceContent.toLowerCase();
      }
      if (shouldAllowThrough(sourceContent, config.getCriteria())) {
        emitter.emit(in);
      }
    }
  }

  private boolean shouldAllowThrough(String sourceContent, String criteria) {
    switch (operator) {
      case EQUAL:
        return sourceContent.equals(criteria);
      case NOT_EQUAL:
        return !sourceContent.equals(criteria);
      case GREATER:
        return (sourceContent.compareTo(criteria) > 0);
      case GREATER_OR_EQUAL:
        return (sourceContent.compareTo(criteria) >= 0);
      case LESS:
        return (sourceContent.compareTo(criteria) < 0);
      case LESS_OR_EQUAL:
        return (sourceContent.compareTo(criteria) <= 0);
      case CONTAINS:
        return sourceContent.contains(criteria);
      case NOT_CONTAINS:
        return !sourceContent.contains(criteria);
      case STARTS_WITH:
        return sourceContent.startsWith(criteria);
      case ENDS_WITH:
        return sourceContent.endsWith(criteria);
      case NOT_STARTS_WITH:
        return !sourceContent.startsWith(criteria);
      case NOT_ENDS_WITH:
        return !sourceContent.endsWith(criteria);
      case MATCH_REGEXP:
        return regexPattern.matcher(sourceContent).find();
      case NOT_MATCH_REGEXP:
        return !regexPattern.matcher(sourceContent).find();
      default:
        throw new IllegalArgumentException("Invalid operator: " + operator);
    }
  }
}
