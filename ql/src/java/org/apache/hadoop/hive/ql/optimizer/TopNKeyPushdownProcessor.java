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

import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TopNKeyOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
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
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import static org.apache.hadoop.hive.ql.optimizer.TopNKeyProcessor.createOperatorBetween;

public class TopNKeyPushdownProcessor implements NodeProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(TopNKeyPushdownProcessor.class);

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
      Object... nodeOutputs) throws SemanticException {
    pushdown((TopNKeyOperator) nd);
    return null;
  }

  private void pushdown(TopNKeyOperator topNKeyOperator) throws SemanticException {
    Operator<? extends OperatorDesc> parentOperator = topNKeyOperator.getParentOperators().get(0);
    LOG.info("p: " + topNKeyOperator.getParentOperators());

    switch (parentOperator.getType()) {
      case SELECT:
        pushdownThroughSelect(topNKeyOperator);
        break;

      case UNION:
        pushdownThroughUnion(topNKeyOperator);
        break;

      case FILTER:
      case FORWARD:
      case LIMIT:
        pushdownThroughFilter(topNKeyOperator);
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
        JoinOperator joinOperator = (JoinOperator) parentOperator;
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
    }
  }

  private void pushdownThroughFilter(TopNKeyOperator topNKeyOperator) throws SemanticException {
    final Operator<? extends OperatorDesc> filterOperator = topNKeyOperator.getParentOperators().get(0);
    final TopNKeyDesc topNKeyDesc = topNKeyOperator.getConf();
    final TopNKeyDesc newTopNKeyDesc = new TopNKeyDesc(topNKeyDesc.getTopN(),
        topNKeyDesc.getColumnSortOrder(), topNKeyDesc.getKeyColumns());
    filterOperator.removeChildAndAdoptItsChildren(topNKeyOperator);
    pushdown(createOperatorBetween(filterOperator.getParentOperators().get(0), filterOperator,
        newTopNKeyDesc));
  }

  private void pushdownThroughUnion(TopNKeyOperator topNKeyOperator) throws SemanticException {
    final UnionOperator unionOperator = (UnionOperator) topNKeyOperator.getParentOperators().get(0);
    final TopNKeyDesc topNKeyDesc = topNKeyOperator.getConf();
    final TopNKeyDesc newTopNKeyDesc = new TopNKeyDesc(topNKeyDesc.getTopN(),
        topNKeyDesc.getColumnSortOrder(), topNKeyDesc.getKeyColumns());
    unionOperator.removeChildAndAdoptItsChildren(topNKeyOperator);

    for (Operator<? extends OperatorDesc> grandParent :
        new ArrayList<>(unionOperator.getParentOperators())) {
      pushdown(createOperatorBetween(grandParent, unionOperator, newTopNKeyDesc));
    }
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
      if (tnkKey instanceof ExprNodeConstantDesc) {
        continue;
      }
      if (!selectMap.containsKey(tnkKey.getExprString())) {
        return;
      }
    }

    final TopNKeyDesc newTopNKeyDesc = new TopNKeyDesc(topNKeyDesc.getTopN(),
        topNKeyDesc.getColumnSortOrder(), topNKeyDesc.getKeyColumns());
    selectOperator.removeChildAndAdoptItsChildren(topNKeyOperator);
    final Operator<? extends OperatorDesc> grandParentOperator = selectOperator.getParentOperators().get(0);
    pushdown(createOperatorBetween(grandParentOperator, selectOperator, newTopNKeyDesc));
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
    final List<ExprNodeDesc> tnkKeys = topNKeyDesc.getKeyColumns();
    final Map<String, ExprNodeDesc> gbyMap = groupByDesc.getColumnExprMap();
    if (tnkKeys.size() != gbyMap.size()) {
      return;
    }
    final List<ExprNodeDesc> mappedColumns = new ArrayList<>();
    for (ExprNodeDesc tnkKey : tnkKeys) {
      final String tnkKeyString = tnkKey.getExprString();
      if (!gbyMap.containsKey(tnkKeyString)) {
        return;
      }
      mappedColumns.add(gbyMap.get(tnkKeyString));
    }

    // We can push it and remove it from above GroupBy.
    final TopNKeyDesc newTopNKeyDesc =
        new TopNKeyDesc(topNKeyDesc.getTopN(), topNKeyDesc.getColumnSortOrder(), mappedColumns);
    groupByOperator.removeChildAndAdoptItsChildren(topNKeyOperator);
    pushdown(createOperatorBetween(groupByOperator.getParentOperators().get(0), groupByOperator,
        newTopNKeyDesc));
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

    // If TopNKey expression is same as GroupBy expression
    final List<ExprNodeDesc> tnkKeys = topNKeyDesc.getKeyColumns();
    final Map<String, ExprNodeDesc> rsMap = reduceSinkDesc.getColumnExprMap();
    if (tnkKeys.size() != reduceSinkDesc.getKeyCols().size()) {
      return;
    }

    final List<ExprNodeDesc> mappedColumns = mapColumns(topNKeyDesc.getKeyColumns(), rsMap);
    if (mappedColumns == null) {
      return;
    }

    // We can push it and remove it from above ReduceSink.
    final TopNKeyDesc newTopNKeyDesc =
        new TopNKeyDesc(topNKeyDesc.getTopN(), topNKeyDesc.getColumnSortOrder(), mappedColumns);
    reduceSinkOperator.removeChildAndAdoptItsChildren(topNKeyOperator);
    pushdown(createOperatorBetween(reduceSinkOperator.getParentOperators().get(0),
        reduceSinkOperator, newTopNKeyDesc));
  }

  private static List<ExprNodeDesc> mapColumns(List<ExprNodeDesc> columns,
      Map<String, ExprNodeDesc> rsMap) {
    final List<ExprNodeDesc> mappedColumns = new ArrayList<>();
    for (ExprNodeDesc column : columns) {
      final String columnName = column.getExprString();
      if (!rsMap.containsKey(columnName)) {
        return null;
      }
      mappedColumns.add(rsMap.get(columnName));
    }
    return mappedColumns;
  }

  private void pushdownThroughFullOuterJoin(TopNKeyOperator topNKeyOperator) {
    final Operator<? extends OperatorDesc> parentOperator =
        topNKeyOperator.getParentOperators().get(0);
    /*
     Push through FOJ. Push TopNKey expression without keys to largest input. Keep on top of FOJ.
     */
  }

  private void pushdownThroughLeftOuterJoin(TopNKeyOperator topNKeyOperator) {
    final Operator<? extends OperatorDesc> parentOperator =
        topNKeyOperator.getParentOperators().get(0);
    /*
     Push through LOJ. If TopNKey expression refers fully to expressions from left input, push with
     rewriting of expressions and remove from top of LOJ. If TopNKey expression has a prefix that
     refers to expressions from left input, push with rewriting of those expressions and keep on
     top of LOJ.
     */
  }

  private void pushdownThroughRightOuterJoin(TopNKeyOperator topNKeyOperator) {
    final Operator<? extends OperatorDesc> parentOperator =
        topNKeyOperator.getParentOperators().get(0);
  }

  private void pushdownThroughInnerJoin(TopNKeyOperator topNKeyOperator) throws SemanticException {
    final JoinOperator joinOperator = (JoinOperator) topNKeyOperator.getParentOperators().get(0);
    final JoinCondDesc joinCondDesc = joinOperator.getConf().getConds()[0];
    final List<Operator<? extends OperatorDesc>> joinInputs = joinOperator.getParentOperators();
    final ReduceSinkOperator leftRS = (ReduceSinkOperator) joinInputs.get(0);
    final ReduceSinkOperator rightRS = (ReduceSinkOperator) joinInputs.get(1);
    final ExprNodeDesc joinKey = joinOperator.getConf().getJoinKeys()[0][0];
    LOG.debug("joinOperator.getConf().getJoinKeys()[0]: "+ Arrays.toString(joinOperator.getConf().getJoinKeys()[0]));

    // One key?
    LOG.debug("one key?");
    final TopNKeyDesc topNKeyDesc = topNKeyOperator.getConf();
    if (topNKeyDesc.getKeyColumns().size() != 1) {
      return;
    }
    final ExprNodeDesc tnkKey = joinOperator.getColumnExprMap().get(
        topNKeyDesc.getKeyColumns().get(0).getExprString());

    LOG.debug("j.m: " + joinOperator.getColumnExprMap());
    LOG.debug("t.k: " + topNKeyOperator.getConf().getKeyColumns());

    // Same key?
    LOG.debug("same key? " + joinKey + ", " + tnkKey);
    if (tnkKey == null) {
      return;
    }
    final String tnkKeyString = tnkKey.getExprString();
    if (!joinKey.getExprString().equals(tnkKeyString.substring("VALUE.".length()))) {
      return;
    }

    // Push down
    LOG.debug("pushdown!");

    LOG.debug("left.k: " + leftRS.getConf().getKeyCols());
    LOG.debug("left.m: " + leftRS.getColumnExprMap());
    LOG.debug("right.k: " + rightRS.getConf().getKeyCols());
    LOG.debug("right.m: " + rightRS.getColumnExprMap());

    joinOperator.removeChildAndAdoptItsChildren(topNKeyOperator);

    final List<ExprNodeDesc> leftKeyCols = leftRS.getConf().getKeyCols();
    if (leftKeyCols != null) {
      final Operator<? extends OperatorDesc> leftRSParent = leftRS.getParentOperators().get(0);
      final List<ExprNodeDesc> mappedColumns =
          mapColumns(leftKeyCols, leftRSParent.getColumnExprMap());
      LOG.debug("leftRSParent.s: " + leftRSParent.getSchema());
      final TopNKeyDesc leftTnkDesc =
          new TopNKeyDesc(topNKeyDesc.getTopN(), topNKeyDesc.getColumnSortOrder(), mappedColumns);
      final TopNKeyOperator newTopNKeyOperator =
          createOperatorBetween(leftRSParent, leftRS, leftTnkDesc);
      newTopNKeyOperator.setColumnExprMap(leftRSParent.getColumnExprMap());
//      pushdown(newTopNKeyOperator);
    }

    final List<ExprNodeDesc> rightKeyCols = rightRS.getConf().getKeyCols();
    if (rightKeyCols != null) {
      final Operator<? extends OperatorDesc> rightRSParent = rightRS.getParentOperators().get(0);
      final List<ExprNodeDesc> mappedColumns =
          mapColumns(rightKeyCols, rightRSParent.getColumnExprMap());
      LOG.debug("rightRSParent.s: " + rightRSParent.getSchema());
      final TopNKeyDesc rightTnkDesc =
          new TopNKeyDesc(topNKeyDesc.getTopN(), topNKeyDesc.getColumnSortOrder(), mappedColumns);
      final TopNKeyOperator newTopNKeyOperator =
          createOperatorBetween(rightRSParent, rightRS, rightTnkDesc);
      newTopNKeyOperator.setColumnExprMap(rightRSParent.getColumnExprMap());
//      pushdown(newTopNKeyOperator);
    }
  }
}
