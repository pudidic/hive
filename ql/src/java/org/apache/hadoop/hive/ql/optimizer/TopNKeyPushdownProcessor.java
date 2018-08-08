/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer;

import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TopNKeyOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import static org.apache.hadoop.hive.ql.optimizer.TopNKeyProcessor.copyDown;

public class TopNKeyPushdownProcessor implements NodeProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(TopNKeyPushdownProcessor.class);

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
      Object... nodeOutputs) throws SemanticException {
    pushdown((TopNKeyOperator) nd);
    return null;
  }

  private void pushdown(TopNKeyOperator topNKeyOperator) throws SemanticException {
    final Operator<? extends OperatorDesc> parentOperator =
        topNKeyOperator.getParentOperators().get(0);

    switch (parentOperator.getType()) {
      case SELECT:
        pushdownThroughSelect(topNKeyOperator);
        break;

      case FORWARD:
        moveDown(topNKeyOperator);
        pushdown(topNKeyOperator);
        break;

      case GROUPBY:
        pushdownThroughGroupBy(topNKeyOperator);
        break;

      case REDUCESINK:
        pushdownThroughReduceSink(topNKeyOperator);
        break;

      case MAPJOIN:
      case MERGEJOIN:
      case JOIN:
        CommonJoinOperator<? extends OperatorDesc> joinOperator =
            (CommonJoinOperator<? extends OperatorDesc>) parentOperator;
        JoinCondDesc[] joinConds = joinOperator.getConf().getConds();
        if (joinConds.length == 1) {
          switch (joinConds[0].getType()) {
            case JoinDesc.FULL_OUTER_JOIN:
              pushdownThroughFullOuterJoin(topNKeyOperator);
              break;

            case JoinDesc.LEFT_OUTER_JOIN:
              pushdownThroughLeftOuterJoin(topNKeyOperator);
              break;

            case JoinDesc.RIGHT_OUTER_JOIN:
              pushdownThroughRightOuterJoin(topNKeyOperator);
              break;

            case JoinDesc.INNER_JOIN:
              pushdownThroughInnerJoin(topNKeyOperator);
              break;
          }
        }

      case TOPNKEY:
        if (hasSameTopNKeyDesc(parentOperator, topNKeyOperator.getConf())) {
          parentOperator.removeChildAndAdoptItsChildren(topNKeyOperator);
        }
        break;
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

  private void moveDown(TopNKeyOperator operator) throws SemanticException {
    assert operator.getNumParent() == 1;
    final Operator<? extends OperatorDesc> parent = operator.getParentOperators().get(0);
    final List<Operator<? extends OperatorDesc>> grandParents = parent.getParentOperators();

    parent.removeChildAndAdoptItsChildren(operator);
    for (Operator<? extends OperatorDesc> grandParent : grandParents) {
      grandParent.replaceChild(parent, operator);
    }
    operator.setParentOperators(new ArrayList<>(grandParents));
    operator.setChildOperators(new ArrayList<>(Collections.singletonList(parent)));
    parent.setParentOperators(new ArrayList<>(Collections.singletonList(operator)));
  }

  private void pushdownThroughSelect(TopNKeyOperator topNKeyOperator) throws SemanticException {
    final SelectOperator selectOperator =
        (SelectOperator) topNKeyOperator.getParentOperators().get(0);

    // Check whether TopNKey key columns can be mapped to expressions based on Project input
    final Map<String, ExprNodeDesc> selectMap = selectOperator.getColumnExprMap();
    if (selectMap == null) {
      return;
    }
    final TopNKeyDesc topNKeyDesc = topNKeyOperator.getConf();

    for (ExprNodeDesc tnkKey : topNKeyDesc.getKeyColumns()) {
      final ExprNodeDesc mappedColumn = selectMap.get(tnkKey.getExprString());
      if (mappedColumn == null) {
        if (ExprNodeDescUtils.isConstant(tnkKey)) {
          continue;
        }
        return;
      }
    }

    final List<ExprNodeDesc> mappedColumns =
        mapColumns(topNKeyDesc.getKeyColumns(), selectOperator.getColumnExprMap());
    topNKeyDesc.setColumnSortOrder(topNKeyDesc.getColumnSortOrder());
    topNKeyDesc.setKeyColumns(mappedColumns);

    moveDown(topNKeyOperator);
    pushdown(topNKeyOperator);
  }

  /**
   * Push through GroupBy. No grouping sets. If TopNKey expression is same as GroupBy expression,
   * we can push it and remove it from above GroupBy. If expression in TopNKey shared common prefix
   * with GroupBy, TopNKey could be pushed through GroupBy using that prefix and kept above it.
   * @param topNKeyOperator
   * @return
   */
  private void pushdownThroughGroupBy(TopNKeyOperator topNKeyOperator) throws SemanticException {
    final GroupByOperator groupByOperator =
        (GroupByOperator) topNKeyOperator.getParentOperators().get(0);
    final GroupByDesc groupByDesc = groupByOperator.getConf();
    final TopNKeyDesc topNKeyDesc = topNKeyOperator.getConf();

    // No grouping sets
    if (groupByDesc.isGroupingSetsPresent()) {
      return;
    }

    // If TopNKey expression is same as GroupBy expression
    final List<ExprNodeDesc> mappedColumns = mapColumns(topNKeyDesc.getKeyColumns(),
        groupByDesc.getColumnExprMap());
    if (!ExprNodeDescUtils.isSame(groupByDesc.getKeys(), mappedColumns)) {
      return;
    }

    // We can push it and remove it from above GroupBy.
    final TopNKeyDesc newTopNKeyDesc =
        new TopNKeyDesc(topNKeyDesc.getTopN(), topNKeyDesc.getColumnSortOrder(), mappedColumns);
    groupByOperator.removeChildAndAdoptItsChildren(topNKeyOperator);
    pushdown(copyDown(groupByOperator, newTopNKeyDesc));
  }

  /**
   * Push through ReduceSink. If TopNKey expression is same as ReduceSink expression and order is
   * the same, we can push it and remove it from above ReduceSink. If expression in TopNKey shared
   * common prefix with ReduceSink including same order, TopNKey could be pushed through ReduceSink
   * using that prefix and kept above it.
   * @param topNKeyOperator
   * @return
   */
  private void pushdownThroughReduceSink(TopNKeyOperator topNKeyOperator) throws SemanticException {
    final ReduceSinkOperator reduceSinkOperator =
        (ReduceSinkOperator) topNKeyOperator.getParentOperators().get(0);
    final ReduceSinkDesc reduceSinkDesc = reduceSinkOperator.getConf();
    final TopNKeyDesc topNKeyDesc = topNKeyOperator.getConf();

    // Same order?
    if (!reduceSinkDesc.getOrder().equals(topNKeyDesc.getColumnSortOrder())) {
      return;
    }

    // If TopNKey expression is same as ReduceSink expression
    final List<ExprNodeDesc> mappedColumns = mapColumns(topNKeyDesc.getKeyColumns(),
        reduceSinkDesc.getColumnExprMap());
    if (!ExprNodeDescUtils.isSame(reduceSinkDesc.getKeyCols(), mappedColumns)) {
      return;
    }

    // We can push it and remove it from above ReduceSink.
    final TopNKeyDesc newTopNKeyDesc =
        new TopNKeyDesc(topNKeyDesc.getTopN(), topNKeyDesc.getColumnSortOrder(), mappedColumns);
    reduceSinkOperator.removeChildAndAdoptItsChildren(topNKeyOperator);
    pushdown(copyDown(reduceSinkOperator, newTopNKeyDesc));
  }

  private static List<ExprNodeDesc> mapColumns(List<ExprNodeDesc> columns,
      Map<String, ExprNodeDesc> colExprMap) {
    if (colExprMap == null) {
      return columns;
    }
    final List<ExprNodeDesc> mappedColumns = new ArrayList<>();
    for (ExprNodeDesc column : columns) {
      final String columnName = column.getExprString();
      if (colExprMap.containsKey(columnName)) {
        mappedColumns.add(colExprMap.get(columnName));
      }
    }
    return mappedColumns;
  }

  private void pushdownThroughFullOuterJoin(TopNKeyOperator topNKeyOperator) throws SemanticException {
    final CommonJoinOperator<? extends OperatorDesc> joinOperator =
        (CommonJoinOperator<? extends OperatorDesc>) topNKeyOperator.getParentOperators().get(0);
    final TopNKeyDesc topNKeyDesc = topNKeyOperator.getConf();
    /*
     Push through FOJ. Push TopNKey expression without keys to largest input. Keep on top of FOJ.
     */
    final ReduceSinkOperator leftReduceSink =
        (ReduceSinkOperator) joinOperator.getParentOperators().get(0);
    final ReduceSinkOperator rightReduceSink =
        (ReduceSinkOperator) joinOperator.getParentOperators().get(1);
    final ReduceSinkOperator largetReduceSink;
    final ReduceSinkOperator smallReduceSink;
    if (leftReduceSink.getStatistics().getDataSize() >
        rightReduceSink.getStatistics().getDataSize()) {
      largetReduceSink = leftReduceSink;
      smallReduceSink = rightReduceSink;
    } else {
      largetReduceSink = rightReduceSink;
      smallReduceSink = leftReduceSink;
    }
    final List<ExprNodeDesc> mappedLargeColumns = mapColumns(mapColumns(topNKeyDesc.getKeyColumns(),
        joinOperator.getColumnExprMap()), largetReduceSink.getColumnExprMap());
    final List<ExprNodeDesc> mappedSmallColumns = mapColumns(mapColumns(topNKeyDesc.getKeyColumns(),
        joinOperator.getColumnExprMap()), smallReduceSink.getColumnExprMap());
    mappedSmallColumns.removeAll(mappedLargeColumns);

    final String mappedOrder = mapOrder(topNKeyDesc.getColumnSortOrder(),
        smallReduceSink.getConf().getKeyCols(), mappedSmallColumns);
    final TopNKeyDesc newTopNKeyDesc = new TopNKeyDesc(topNKeyDesc.getTopN(), mappedOrder,
        mappedSmallColumns);
    final TopNKeyOperator newTopNKeyOperator = copyDown(smallReduceSink, newTopNKeyDesc);
    pushdown(newTopNKeyOperator);
  }

  private void pushdownThroughLeftOuterJoin(TopNKeyOperator topNKeyOperator)
      throws SemanticException {
    pushdownThroughLeftOrRightOuterJoin(topNKeyOperator, 0);
  }

  private void pushdownThroughRightOuterJoin(TopNKeyOperator topNKeyOperator)
      throws SemanticException {
    pushdownThroughLeftOrRightOuterJoin(topNKeyOperator, 1);
  }

  private void pushdownThroughLeftOrRightOuterJoin(TopNKeyOperator topNKeyOperator, int position)
      throws SemanticException {
    /*
     Push through LOJ. If TopNKey expression refers fully to expressions from left input, push with
     rewriting of expressions and remove from top of LOJ. If TopNKey expression has a prefix that
     refers to expressions from left input, push with rewriting of those expressions and keep on
     top of LOJ.
     */
    final TopNKeyDesc topNKeyDesc = topNKeyOperator.getConf();
    final CommonJoinOperator<? extends OperatorDesc> joinOperator =
        (CommonJoinOperator<? extends OperatorDesc>) topNKeyOperator.getParentOperators().get(0);
    final List<Operator<? extends OperatorDesc>> joinInputs = joinOperator.getParentOperators();

    // Null order check
    final ReduceSinkOperator reduceSinkOperator = (ReduceSinkOperator) joinInputs.get(position);
    final ReduceSinkDesc reduceSinkDesc = reduceSinkOperator.getConf();
    final String nullOrder = reduceSinkDesc.getNullOrder();
    for (int i = 0; i < nullOrder.length(); i++) {
      if (nullOrder.charAt(i) != 'a') {
        return;
      }
    }

    // Column mapping check
    final List<ExprNodeDesc> mappedColumns = mapColumns(mapColumns(topNKeyDesc.getKeyColumns(),
        joinOperator.getColumnExprMap()), reduceSinkOperator.getColumnExprMap());
    if (mappedColumns.isEmpty()) {
      return;
    }
    final String mappedOrder = mapOrder(topNKeyDesc.getColumnSortOrder(),
        reduceSinkDesc.getKeyCols(), mappedColumns);

    final TopNKeyDesc newTopNKeyDesc = new TopNKeyDesc(topNKeyDesc.getTopN(), mappedOrder,
        mappedColumns);
    final TopNKeyOperator newTopNKeyOperator = copyDown(reduceSinkOperator, newTopNKeyDesc);
    pushdown(newTopNKeyOperator);

    // If all columns are mapped, remove from top
    if (topNKeyDesc.getKeyColumns().size() == mappedColumns.size()) {
      joinOperator.removeChildAndAdoptItsChildren(topNKeyOperator);
    }
  }

  private void pushdownThroughInnerJoin(TopNKeyOperator topNKeyOperator) throws SemanticException {
    final CommonJoinOperator<? extends OperatorDesc> joinOperator =
        (CommonJoinOperator<? extends OperatorDesc>) topNKeyOperator.getParentOperators().get(0);
    final List<Operator<? extends OperatorDesc>> joinInputs = joinOperator.getParentOperators();
    final TopNKeyDesc topNKeyDesc = topNKeyOperator.getConf();

    // Push down
//    joinOperator.removeChildAndAdoptItsChildren(topNKeyOperator);

    for (Operator<? extends OperatorDesc> joinInput : joinInputs) {
      final ReduceSinkOperator reduceSink = (ReduceSinkOperator) joinInput;
      final List<ExprNodeDesc> mappedColumns = mapColumns(mapColumns(topNKeyDesc.getKeyColumns(),
          joinOperator.getColumnExprMap()), reduceSink.getColumnExprMap());
      if (!mappedColumns.isEmpty()) {
        final String mappedOrder = mapOrder(topNKeyDesc.getColumnSortOrder(),
            reduceSink.getConf().getKeyCols(), mappedColumns);
        final TopNKeyDesc newTopNKeyDesc = new TopNKeyDesc(topNKeyDesc.getTopN(), mappedOrder,
            mappedColumns);
        final TopNKeyOperator newTopNKeyOperator = copyDown(reduceSink, newTopNKeyDesc);
        pushdown(newTopNKeyOperator);
      }
    }
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
