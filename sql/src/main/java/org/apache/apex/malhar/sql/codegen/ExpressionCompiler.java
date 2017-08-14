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
package org.apache.apex.malhar.sql.codegen;

import java.util.List;
import java.util.regex.Matcher;

import org.apache.apex.malhar.sql.operators.OperatorUtils;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Statement;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.collect.ImmutableList;

/**
 * Converts calcite expression of type {@link RexNode} to quasi-Java expression which can be used
 * with {@link org.apache.apex.malhar.lib.util.PojoUtils}
 *
 * @since 3.6.0
 */
@InterfaceStability.Evolving
public class ExpressionCompiler
{
  private final RexBuilder rexBuilder;

  public ExpressionCompiler(RexBuilder rexBuilder)
  {
    this.rexBuilder = rexBuilder;
  }

  /**
   * Create quasi-Java expression from given {@link RexNode}
   *
   * @param node Expression in the form of {@link RexNode}
   * @param inputRowType Input Data type to expression in the form of {@link RelDataType}
   * @param outputRowType Output data type of expression in the form of {@link RelDataType}
   *
   * @return Returns quasi-Java expression
   */
  public String getExpression(RexNode node, RelDataType inputRowType, RelDataType outputRowType)
  {
    final RexProgramBuilder programBuilder = new RexProgramBuilder(inputRowType, rexBuilder);
    programBuilder.addProject(node, null);
    final RexProgram program = programBuilder.getProgram();

    final BlockBuilder builder = new BlockBuilder();
    final JavaTypeFactory javaTypeFactory = (JavaTypeFactory)rexBuilder.getTypeFactory();

    final RexToLixTranslator.InputGetter inputGetter = new RexToLixTranslator.InputGetterImpl(ImmutableList
        .of(Pair.<Expression, PhysType>of(Expressions.variable(Object[].class, "inputValues"),
        PhysTypeImpl.of(javaTypeFactory, inputRowType, JavaRowFormat.ARRAY, false))));
    final Function1<String, RexToLixTranslator.InputGetter> correlates =
        new Function1<String, RexToLixTranslator.InputGetter>()
      {
        public RexToLixTranslator.InputGetter apply(String a0)
        {
          throw new UnsupportedOperationException();
        }
      };

    final List<Expression> list = RexToLixTranslator.translateProjects(program, javaTypeFactory, builder,
        PhysTypeImpl.of(javaTypeFactory, outputRowType, JavaRowFormat.ARRAY, false), null, inputGetter, correlates);

    for (int i = 0; i < list.size(); i++) {
      Statement statement = Expressions.statement(list.get(i));
      builder.add(statement);
    }

    return finalizeExpression(builder.toBlock(), inputRowType);
  }

  private String finalizeExpression(BlockStatement blockStatement, RelDataType inputRowType)
  {
    String s = Expressions.toString(blockStatement.statements.get(0));
    int idx = 0;
    for (RelDataTypeField field : inputRowType.getFieldList()) {
      String fieldName = OperatorUtils.getFieldName(field);
      s = s.replaceAll(String.format("inputValues\\[%d\\]", idx++), "\\{\\$." + Matcher.quoteReplacement(fieldName) + "\\}");
    }

    return s.substring(0, s.length() - 2);
  }
}
