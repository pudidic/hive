/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer;

import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TopNKeyOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import static org.apache.hadoop.hive.ql.plan.ExprNodeDesc.ExprNodeDescEqualityWrapper.transform;

public class TopNKeyPushdown implements NodeProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(TopNKeyPushdown.class);

  public TopNKeyPushdown() {
  }

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
      Object... nodeOutputs) throws SemanticException {

    checkAndPushdown((TopNKeyOperator) nd);
    return null;
  }

  private List<ExprNodeDesc> mapColumns(Map<String, ExprNodeDesc> mapping, List<ExprNodeDesc> cols) {
    List<ExprNodeDesc> colsInOperator = new ArrayList<>();
    for (ExprNodeDesc col : cols) {
      String exprString = col.getExprString();
      if (mapping.containsKey(exprString)) {
        colsInOperator.add(mapping.get(exprString));
      }
    }
    return colsInOperator;
  }

  private void checkAndPushdown(TopNKeyOperator topNKeyOperator) throws SemanticException {
    Operator<? extends OperatorDesc> parentOperator = topNKeyOperator.getParentOperators().get(0);
    switch (parentOperator.getType()) {
      case GROUPBY:
        handleGroupBy(topNKeyOperator);
        break;

      case SELECT:
        handleSelect(topNKeyOperator);
        break;

      case FILTER:
      case FORWARD:
      case LIMIT:
        moveDown(parentOperator, topNKeyOperator);
        checkAndPushdown(topNKeyOperator);
        break;

      case JOIN:
      case MERGEJOIN:
      case MAPJOIN:
        handleJoin((CommonJoinOperator<? extends OperatorDesc>) parentOperator, topNKeyOperator);
        break;

      case TOPNKEY:
        if (hasSameTopNKeyDesc(parentOperator, topNKeyOperator.getConf())) {
          parentOperator.removeChildAndAdoptItsChildren(topNKeyOperator);
        }
        break;

    }
  }

  private void handleJoin(CommonJoinOperator<? extends OperatorDesc> joinOperator,
      TopNKeyOperator topNKeyOperator) throws SemanticException {

    TopNKeyDesc topNKeyDesc = topNKeyOperator.getConf();
    JoinDesc joinDesc = joinOperator.getConf();

    // Supports inner joins only
    for (JoinCondDesc cond : joinDesc.getConds()) {
      if (cond.getType() != JoinDesc.INNER_JOIN) {
        return;
      }
    }

    for (Operator<? extends OperatorDesc> parentOperator : joinOperator.getParentOperators()) {
      ReduceSinkOperator reduceSinkOperator = (ReduceSinkOperator) parentOperator;
      ReduceSinkDesc reduceSinkDesc = reduceSinkOperator.getConf();

      List<ExprNodeDesc> mappedKeyColumns = mapColumns(reduceSinkDesc.getColumnExprMap(),
          mapColumns(joinDesc.getColumnExprMap(), topNKeyDesc.getKeyColumns()));

      // There should be mapped key cols
      if (mappedKeyColumns.isEmpty()) {
        return;
      }

      TopNKeyDesc newTopNKeyDesc = new TopNKeyDesc(topNKeyDesc.getTopN(),
          mapOrder(topNKeyDesc.getColumnSortOrder(), reduceSinkDesc.getKeyCols(), mappedKeyColumns),
          mappedKeyColumns);

      TopNKeyOperator newTopNKeyOperator = copyDown(newTopNKeyDesc, reduceSinkOperator);
      if (newTopNKeyOperator != null) {
        checkAndPushdown(newTopNKeyOperator);
      }
    }
  }

  private boolean hasSameTopNKeyDesc(Operator<? extends OperatorDesc> operator, TopNKeyDesc desc) {
    if (operator instanceof TopNKeyOperator) {
      TopNKeyOperator topNKeyOperator = (TopNKeyOperator) operator;
      TopNKeyDesc opDesc = topNKeyOperator.getConf();
      if (opDesc.isSame(desc)) {
        return true;
      }
    }
    return false;
  }

  private void handleSelect(TopNKeyOperator topNKeyOperator) throws SemanticException {

    TopNKeyDesc topNKeyDesc = topNKeyOperator.getConf();
    SelectOperator selectOperator = (SelectOperator) topNKeyOperator.getParentOperators().get(0);
    SelectDesc selectDesc = selectOperator.getConf();

    List<ExprNodeDesc> mappedKeyColumns =
        mapColumns(selectDesc.getColumnExprMap(), topNKeyDesc.getKeyColumns());
    for (ExprNodeDesc tnkKey : topNKeyDesc.getKeyColumns()) {
      if (tnkKey instanceof ExprNodeConstantDesc) {
        continue;
      }
      if (!selectDesc.getColumnExprMap().containsKey(tnkKey.getExprString())) {
        return;
      }
    }

    topNKeyDesc.setColumnSortOrder(mapOrder(topNKeyDesc.getColumnSortOrder(),
        selectDesc.getColList(), mappedKeyColumns));
    topNKeyDesc.setKeyColumns(mappedKeyColumns);
    topNKeyOperator.setSchema(new RowSchema(selectOperator.getSchema()));

    moveDown(selectOperator, topNKeyOperator);
    checkAndPushdown(topNKeyOperator);
  }

  private void handleGroupBy(TopNKeyOperator topNKeyOperator) throws SemanticException {

    TopNKeyDesc topNKeyDesc = topNKeyOperator.getConf();
    GroupByOperator groupByOperator = (GroupByOperator) topNKeyOperator.getParentOperators().get(0);
    GroupByDesc groupByDesc = groupByOperator.getConf();

    List<ExprNodeDesc> mappedKeyColumns = mapColumns(groupByDesc.getColumnExprMap(),
        topNKeyDesc.getKeyColumns());
    if (!transform(groupByDesc.getKeys()).containsAll(transform(mappedKeyColumns))) {
      return;
    }

    topNKeyDesc.setColumnSortOrder(mapOrder(topNKeyDesc.getColumnSortOrder(), groupByDesc.getKeys(),
        mappedKeyColumns));
    topNKeyDesc.setKeyColumns(mappedKeyColumns);
    topNKeyOperator.setSchema(new RowSchema(groupByOperator.getSchema()));

    moveDown(groupByOperator, topNKeyOperator);
    checkAndPushdown(topNKeyOperator);
  }

  private void moveDown(Operator<? extends OperatorDesc> baseOperator,
      TopNKeyOperator topNKeyOperator) throws SemanticException {

    LOG.debug("Move down through " + baseOperator);

    // Before: PARENT ---> BASE ---> TNK ---> CHILD
    // After:  PARENT -1-> TNK -2-> BASE -3-> CHILD

    List<Operator<? extends OperatorDesc>> parentOperators = baseOperator.getParentOperators();
    List<Operator<? extends OperatorDesc>> childOperators = topNKeyOperator.getChildOperators();

    // PARENT -1-> TNK
    for (Operator<? extends OperatorDesc> parentOperator : parentOperators) {
      parentOperator.replaceChild(baseOperator, topNKeyOperator);
    }
    topNKeyOperator.setParentOperators(new ArrayList<>(parentOperators));

    // TNK -2-> BASE
    topNKeyOperator.setChildOperators(new ArrayList<>(Collections.singletonList(baseOperator)));
    baseOperator.setParentOperators(new ArrayList<>(Collections.singletonList(topNKeyOperator)));

    // BASE -3-> CHILD
    baseOperator.setChildOperators(new ArrayList<>(childOperators));
    for (Operator<? extends OperatorDesc> childOperator : childOperators) {
      childOperator.replaceParent(topNKeyOperator, baseOperator);
    }
  }

  private TopNKeyOperator copyDown(TopNKeyDesc newDesc, Operator<? extends OperatorDesc>
      baseOperator) {

    Operator<? extends OperatorDesc> parentOperator = baseOperator.getParentOperators().get(0);

    LOG.debug("New top n key operator between " + baseOperator + " and " + parentOperator);

    // Before: PARENT ------------> BASE
    // After:  PARENT -1-> TNK -2-> BASE

    Operator<? extends OperatorDesc> newOperator =
        OperatorFactory.get(baseOperator.getCompilationOpContext(), newDesc,
            parentOperator.getSchema());

    // PARENT -1-> TNK
    parentOperator.replaceChild(baseOperator, newOperator);
    newOperator.setParentOperators(new ArrayList<>(Collections.singletonList(parentOperator)));

    // TNK -2-> BASE
    newOperator.setChildOperators(new ArrayList<>(Collections.singletonList(baseOperator)));
    baseOperator.setParentOperators(new ArrayList<>(Collections.singletonList(newOperator)));

    return (TopNKeyOperator) newOperator;
  }

  private static String mapOrder(String order, List<ExprNodeDesc> parentCols, List<ExprNodeDesc>
      mappedCols) {

    StringBuilder builder = new StringBuilder();
    int index = 0;
    for (ExprNodeDesc mappedCol : mappedCols) {
      if (parentCols.contains(mappedCol)) {
        builder.append(order.charAt(index++));
      } else {
        builder.append("+");
      }
    }
    return builder.toString();
  }
}
