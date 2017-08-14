/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.sql.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.apex.malhar.sql.codegen.ExpressionCompiler;
import org.apache.apex.malhar.sql.operators.FilterTransformOperator;
import org.apache.apex.malhar.sql.operators.InnerJoinOperator;
import org.apache.apex.malhar.sql.operators.OperatorUtils;
import org.apache.apex.malhar.sql.schema.ApexSQLTable;
import org.apache.apex.malhar.sql.schema.TupleSchemaRegistry;
import org.apache.apex.malhar.sql.table.Endpoint;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.stream.Delta;
import org.apache.calcite.rel.stream.LogicalDelta;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;

/**
 * This class defines how to populate DAG of Apex for the relational nodes of SQL Calcite
 *
 * @since 3.6.0
 */
@InterfaceStability.Evolving
public abstract class ApexRelNode
{
  public static Map<Class, ApexRelNode> relNodeMapping = ImmutableMap.<Class, ApexRelNode>builder()
      .put(LogicalDelta.class, new ApexDeltaRel())
      .put(LogicalTableScan.class, new ApexTableScanRel())
      .put(LogicalTableModify.class, new ApexTableModifyRel())
      .put(LogicalProject.class, new ApexProjectRel())
      .put(LogicalFilter.class, new ApexFilterRel())
      .put(LogicalJoin.class, new ApexJoinRel())
      .build();

  public abstract RelInfo visit(RelContext context, RelNode node, List<RelInfo> inputStreams);

  public static class RelContext
  {
    public DAG dag;
    public JavaTypeFactory typeFactory;
    public TupleSchemaRegistry schemaRegistry;

    public RelContext(DAG dag, JavaTypeFactory typeFactory, TupleSchemaRegistry registry)
    {
      this.dag = dag;
      this.typeFactory = typeFactory;
      this.schemaRegistry = registry;
    }
  }

  /**
   * This is visitor for {@link Delta} to emit the data to {@link ConsoleOutputOperator}.
   */
  private static class ApexDeltaRel extends ApexRelNode
  {
    @Override
    public RelInfo visit(RelContext context, RelNode node, List<RelInfo> inputStreams)
    {
      Delta delta = (Delta)node;

      ConsoleOutputOperator console = context.dag
          .addOperator(OperatorUtils.getUniqueOperatorName(delta.getRelTypeName()), ConsoleOutputOperator.class);
      console.setStringFormat("Delta Record: %s");

      return new RelInfo("Delta", Lists.<Operator.InputPort>newArrayList(console.input), console, null,
          delta.getRowType());
    }
  }

  /**
   * This is visitor for {@link TableScan} for adding operators to DAG.
   */
  private static class ApexTableScanRel extends ApexRelNode
  {
    @Override
    public RelInfo visit(RelContext context, RelNode node, List<RelInfo> inputStreams)
    {
      TableScan scan = (TableScan)node;
      ApexSQLTable table = scan.getTable().unwrap(ApexSQLTable.class);
      Endpoint endpoint = table.getEndpoint();
      return endpoint.populateInputDAG(context.dag, context.typeFactory);
    }
  }

  /**
   * This is visitor for {@link TableModify} for adding operators to DAG.
   */
  private static class ApexTableModifyRel extends ApexRelNode
  {
    @Override
    public RelInfo visit(RelContext context, RelNode node, List<RelInfo> inputStreams)
    {
      /**
       * Only INSERT is allowed as it representation destination for DAG processing. Other types like UPDATE, DELETE,
       * MERGE does not represent the same.
       */

      TableModify modify = (TableModify)node;
      Preconditions.checkArgument(modify.isInsert(), "Only INSERT allowed for table modify");

      ApexSQLTable table = modify.getTable().unwrap(ApexSQLTable.class);

      Endpoint endpoint = table.getEndpoint();
      return endpoint.populateOutputDAG(context.dag, context.typeFactory);
    }
  }

  /**
   * This is visitor for {@link Project} for adding operators to DAG.
   */
  private static class ApexProjectRel extends ApexRelNode
  {
    @Override
    public RelInfo visit(RelContext context, RelNode node, List<RelInfo> inputStreams)
    {
      Project project = (Project)node;
      if (inputStreams.size() == 0 || inputStreams.size() > 1) {
        throw new UnsupportedOperationException("Project is a SingleRel");
      }

      FilterTransformOperator operator = context.dag
          .addOperator(OperatorUtils.getUniqueOperatorName(project.getRelTypeName()), FilterTransformOperator.class);
      Map<String, String> expMap = new HashMap<>();
      ExpressionCompiler compiler = new ExpressionCompiler(new RexBuilder(project.getCluster().getTypeFactory()));

      for (Pair<RelDataTypeField, RexNode> pair : Pair.zip(project.getRowType().getFieldList(),
          project.getProjects())) {
        String fieldName = OperatorUtils.getFieldName(pair.left);
        String expression = compiler.getExpression(pair.right, project.getInput().getRowType(), project.getRowType());
        expMap.put(fieldName, expression);
      }
      operator.setExpressionMap(expMap);

      return new RelInfo("Project", Lists.<Operator.InputPort>newArrayList(operator.input), operator, operator.output,
          project.getRowType());
    }
  }

  /**
   * This is visitor for {@link Filter} for adding operators to DAG.
   */
  private static class ApexFilterRel extends ApexRelNode
  {
    @Override
    public RelInfo visit(RelContext context, RelNode node, List<RelInfo> inputStreams)
    {
      Filter filter = (Filter)node;
      if (inputStreams.size() == 0 || inputStreams.size() > 1) {
        throw new UnsupportedOperationException("Filter is a SingleRel");
      }

      FilterTransformOperator operator = context.dag
          .addOperator(OperatorUtils.getUniqueOperatorName(filter.getRelTypeName()), FilterTransformOperator.class);
      ExpressionCompiler compiler = new ExpressionCompiler(new RexBuilder(filter.getCluster().getTypeFactory()));
      String expression = compiler.getExpression(filter.getCondition(), filter.getInput().getRowType(),
          filter.getRowType());

      Map<String, String> expMap = new HashMap<>();
      for (Pair<RelDataTypeField, RelDataTypeField> pair : Pair.zip(filter.getInput().getRowType().getFieldList(),
          filter.getRowType().getFieldList())) {
        String leftName = OperatorUtils.getFieldName(pair.left);
        String rightName = OperatorUtils.getFieldName(pair.right);
        expMap.put(leftName, rightName);
      }
      operator.setExpressionMap(expMap);
      operator.setCondition(expression);

      return new RelInfo("Filter", Lists.<Operator.InputPort>newArrayList(operator.input), operator, operator.output,
          filter.getRowType());
    }
  }

  /**
   * This is visitor for {@link Join} for adding operators to DAG.
   */
  private static class ApexJoinRel extends ApexRelNode
  {

    @Override
    public RelInfo visit(RelContext context, RelNode node, List<RelInfo> inputStreams)
    {
      Join join = (Join)node;
      if (inputStreams.size() != 2) {
        throw new UnsupportedOperationException("Join is a BiRel");
      }

      if ((join.getJoinType() == JoinRelType.FULL) || (join.getJoinType() == JoinRelType.LEFT) ||
          (join.getJoinType() == JoinRelType.RIGHT)) {
        throw new UnsupportedOperationException("Outer joins are not supported");
      }

      final List<Integer> leftKeys = new ArrayList<>();
      final List<Integer> rightKeys = new ArrayList<>();

      RexNode remaining =
          RelOptUtil.splitJoinCondition(join.getLeft(), join.getRight(), join.getCondition(), leftKeys, rightKeys);

      if (leftKeys.size() != rightKeys.size()) {
        throw new RuntimeException("Unexpected condition reached. Left and right condition count should be same");
      }

      if (leftKeys.size() == 0) {
        throw new UnsupportedOperationException("Theta joins are not supported.");
      }

      RelInfo relInfo = addInnerJoinOperator(join, leftKeys, rightKeys, context);

      if (!remaining.isAlwaysTrue()) {
        relInfo = addJoinFilter(join, remaining, relInfo, context);
      }

      return relInfo;
    }

    private RelInfo addJoinFilter(Join join, RexNode remaining, RelInfo relInfo, RelContext context)
    {
      FilterTransformOperator operator = context.dag
          .addOperator(OperatorUtils.getUniqueOperatorName(join.getRelTypeName() + "_Filter"),
          FilterTransformOperator.class);
      ExpressionCompiler compiler = new ExpressionCompiler(new RexBuilder(join.getCluster().getTypeFactory()));
      String expression = compiler.getExpression(remaining, join.getRowType(), join.getRowType());

      Map<String, String> expMap = new HashMap<>();
      for (Pair<RelDataTypeField, RelDataTypeField> pair : Pair.zip(join.getRowType().getFieldList(),
          join.getRowType().getFieldList())) {
        String leftName = OperatorUtils.getFieldName(pair.left);
        String rightName = OperatorUtils.getFieldName(pair.right);
        expMap.put(leftName, rightName);
      }
      operator.setExpressionMap(expMap);
      operator.setCondition(expression);

      String streamName = OperatorUtils.getUniqueStreamName(join.getRelTypeName() + "_Join", join.getRelTypeName() +
          "_Filter");
      Class schema = TupleSchemaRegistry.getSchemaForRelDataType(context.schemaRegistry, streamName,
          relInfo.getOutRelDataType());
      context.dag.setOutputPortAttribute(relInfo.getOutPort(), Context.PortContext.TUPLE_CLASS, schema);
      context.dag.setInputPortAttribute(operator.input, Context.PortContext.TUPLE_CLASS, schema);
      context.dag.addStream(streamName, relInfo.getOutPort(), operator.input);

      return new RelInfo("Join", relInfo.getInputPorts(), operator, operator.output, join.getRowType());
    }

    private RelInfo addInnerJoinOperator(Join join, List<Integer> leftKeys, List<Integer> rightKeys, RelContext context)
    {
      String leftKeyExpression = null;
      String rightKeyExpression = null;
      for (Integer leftKey : leftKeys) {
        String name = OperatorUtils.getValidFieldName(join.getLeft().getRowType().getFieldList().get(leftKey));
        leftKeyExpression = (leftKeyExpression == null) ? name : leftKeyExpression + " + " + name;
      }

      for (Integer rightKey : rightKeys) {
        String name = OperatorUtils.getValidFieldName(join.getRight().getRowType().getFieldList().get(rightKey));
        rightKeyExpression = (rightKeyExpression == null) ? name : rightKeyExpression + " + " + name;
      }

      String includeFieldStr = "";
      boolean first = true;
      for (RelDataTypeField field : join.getLeft().getRowType().getFieldList()) {
        if (first) {
          first = false;
        } else {
          includeFieldStr += ",";
        }
        includeFieldStr += OperatorUtils.getValidFieldName(field);
      }
      includeFieldStr += ";";
      first = true;
      for (RelDataTypeField field : join.getRight().getRowType().getFieldList()) {
        if (first) {
          first = false;
        } else {
          includeFieldStr += ",";
        }
        includeFieldStr += OperatorUtils.getValidFieldName(field);
      }

      InnerJoinOperator innerJoin = context.dag.addOperator(OperatorUtils.getUniqueOperatorName(join.getRelTypeName()),
          InnerJoinOperator.class);
      innerJoin.setExpiryTime(1L);
      // Number of buckets is set to 47000 because this is rounded number closer to sqrt of MAXINT. This guarantees
      // even distribution of keys across buckets.
      innerJoin.setNoOfBuckets(47000);
      innerJoin.setTimeFieldsStr("");

      innerJoin.setLeftKeyExpression(leftKeyExpression);
      innerJoin.setRightKeyExpression(rightKeyExpression);

      innerJoin.setIncludeFieldStr(includeFieldStr);

      return new RelInfo("Join", Lists.<Operator.InputPort>newArrayList(innerJoin.input1, innerJoin.input2), innerJoin,
          innerJoin.outputPort, join.getRowType());
    }
  }
}
