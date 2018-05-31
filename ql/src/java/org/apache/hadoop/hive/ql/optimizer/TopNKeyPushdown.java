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
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.TopNKeyOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
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

  private TopNKeyOperator operator;
  private TopNKeyDesc desc;

  public TopNKeyPushdown() {
  }

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
      Object... nodeOutputs) throws SemanticException {

    operator = (TopNKeyOperator) nd;
    desc = operator.getConf();

    int depth = stack.size() - 1;
    int delta;
    while ((delta = checkAndPushdown(stack, depth)) > 0) {
      depth -= delta;
    }

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

  private int checkAndPushdown(Stack<Node> stack, int depth) {

    Operator<? extends OperatorDesc> parentOperator = (Operator<? extends OperatorDesc>)
        stack.get(depth - 1);

    if (parentOperator instanceof TableScanOperator) {
      return 0;
    }

    if (parentOperator instanceof GroupByOperator) {
      return handleGroupBy(parentOperator);
    } else if (parentOperator instanceof SelectOperator) {
      return handleSelect(parentOperator);
    } else if (parentOperator instanceof FilterOperator) {
      moveDown(parentOperator);
      return 1;
    } else if (parentOperator instanceof CommonJoinOperator) {
      Operator<? extends OperatorDesc> grandParentOperator =
          (Operator<? extends OperatorDesc>) stack.get(depth - 2);
      Operator<? extends OperatorDesc> grandGrandParentOperator =
          (Operator<? extends OperatorDesc>) stack.get(depth - 3);
      return handleJoin(parentOperator, grandParentOperator, grandGrandParentOperator);
    }
    return 0;
  }

  private int handleJoin(Operator<? extends OperatorDesc> parentOperator,
      Operator<? extends OperatorDesc> grandParentOperator,
      Operator<? extends OperatorDesc> grandGrandParentOperator) {

    CommonJoinOperator joinOperator = (CommonJoinOperator) parentOperator;
    JoinDesc joinDesc = (JoinDesc) joinOperator.getConf();

    // Supports inner joins only
    if (!joinDesc.isNoOuterJoin()) {
      return 0;
    }

    ReduceSinkOperator reduceSinkOperator = (ReduceSinkOperator) grandParentOperator;
    ReduceSinkDesc reduceSinkDesc = reduceSinkOperator.getConf();

    List<ExprNodeDesc> mappedKeyColumns = mapColumns(reduceSinkDesc.getColumnExprMap(),
        mapColumns(joinDesc.getColumnExprMap(), desc.getKeyColumns()));

    // There should be mapped key cols
    if (mappedKeyColumns.isEmpty()) {
      return 0;
    }

    TopNKeyDesc newTopNKeyDesc = new TopNKeyDesc(desc.getTopN(),
        mapOrder(desc.getColumnSortOrder(), reduceSinkDesc.getKeyCols(), mappedKeyColumns),
        mappedKeyColumns);

    // Avoid duplicates
    if (grandGrandParentOperator instanceof TopNKeyOperator) {
      TopNKeyOperator grandGrandParentTnkOp = (TopNKeyOperator) grandGrandParentOperator;
      TopNKeyDesc grandGrandParentTnkDesc = grandGrandParentTnkOp.getConf();
      if (grandGrandParentTnkDesc.isSame(newTopNKeyDesc)) {
        return 0;
      }
    }

    operator = copyDown(newTopNKeyDesc, reduceSinkOperator);
    desc = operator.getConf();
    return 2;
  }

  private int handleSelect(Operator<? extends OperatorDesc> parentOperator) {

    SelectOperator selectOperator = (SelectOperator) parentOperator;
    SelectDesc selectDesc = selectOperator.getConf();

    List<ExprNodeDesc> mappedKeyColumns =
        mapColumns(parentOperator.getColumnExprMap(), desc.getKeyColumns());
    if (!transform(selectDesc.getColList()).containsAll(transform(mappedKeyColumns))) {
      return 0;
    }

    desc.setColumnSortOrder(mapOrder(desc.getColumnSortOrder(),
        selectDesc.getColList(), mappedKeyColumns));
    desc.setKeyColumns(mappedKeyColumns);
    operator.setSchema(new RowSchema(selectOperator.getSchema()));

    moveDown(parentOperator);
    return 1;
  }

  private int handleGroupBy(Operator<? extends OperatorDesc> parentOperator) {

    GroupByOperator groupByOperator = (GroupByOperator) parentOperator;
    GroupByDesc groupByDesc = groupByOperator.getConf();

    List<ExprNodeDesc> mappedKeyColumns = mapColumns(parentOperator.getColumnExprMap(),
        desc.getKeyColumns());
    if (!transform(groupByDesc.getKeys()).containsAll(transform(mappedKeyColumns))) {
      return 0;
    }

    desc.setColumnSortOrder(mapOrder(desc.getColumnSortOrder(),
        groupByDesc.getKeys(), mappedKeyColumns));
    desc.setKeyColumns(mappedKeyColumns);
    operator.setSchema(new RowSchema(groupByOperator.getSchema()));

    moveDown(parentOperator);
    return 1;
  }

  private void moveDown(Operator<? extends OperatorDesc> baseOperator) {
    LOG.debug("Move down through " + baseOperator);

    // Before: PARENT ---> BASE ---> TNK ---> CHILD
    // After:  PARENT -1-> TNK -2-> BASE -3-> CHILD

    List<Operator<? extends OperatorDesc>> parentOperators = baseOperator.getParentOperators();
    List<Operator<? extends OperatorDesc>> childOperators = operator.getChildOperators();

    // PARENT -1-> TNK
    for (Operator<? extends OperatorDesc> parentOperator : parentOperators) {
      parentOperator.replaceChild(baseOperator, operator);
    }
    operator.setParentOperators(new ArrayList<>(parentOperators));

    // TNK -2-> BASE
    operator.setChildOperators(new ArrayList<>(Collections.singletonList(baseOperator)));
    baseOperator.setParentOperators(new ArrayList<>(Collections.singletonList(operator)));

    // BASE -3-> CHILD
    baseOperator.setChildOperators(new ArrayList<>(childOperators));
    for (Operator<? extends OperatorDesc> childOperator : childOperators) {
      childOperator.replaceParent(operator, baseOperator);
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
