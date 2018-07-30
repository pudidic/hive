/**
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
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TopNKeyOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
        return;

      case UNION:
        pushdownThroughUnion(topNKeyOperator);
        return;

      case FILTER:
      case FORWARD:
      case LIMIT:
        pushdownThroughFilter(topNKeyOperator);
        return;

      case GROUPBY:
        pushdownThroughGroupBy(topNKeyOperator);
        return;

      case REDUCESINK:
        pushdownThroughReduceSink(topNKeyOperator);
        return;

      case MAPJOIN:
      case MERGEJOIN:
      case JOIN:
        JoinOperator joinOperator = (JoinOperator) parentOperator;
        JoinCondDesc[] joinConds = joinOperator.getConf().getConds();
        if (joinConds.length == 1) {
          switch (joinConds[0].getType()) {
            case JoinDesc.FULL_OUTER_JOIN:
              pushdownThroughFullOuterJoin(topNKeyOperator);
              return;
            case JoinDesc.LEFT_OUTER_JOIN:
              pushdownThroughLeftOuterJoin(topNKeyOperator);
              return;
            case JoinDesc.RIGHT_OUTER_JOIN:
              pushdownThroughRightOuterJoin(topNKeyOperator);
              return;
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
    final SelectOperator selectOperator = (SelectOperator) topNKeyOperator.getParentOperators().get(0);
    LOG.debug("select.schema: " + selectOperator.getSchema());

    final TopNKeyDesc topNKeyDesc = topNKeyOperator.getConf();
    LOG.debug("topNKey.schema: " + topNKeyOperator.getSchema());

    // Check whether TopNKey key columns can be mapped to expressions based on Project input
    final Map<String, ExprNodeDesc> selectMap = selectOperator.getColumnExprMap();
    if (selectMap == null) {
      return;
    }
    final List<ExprNodeDesc> mappedKeyColumns = new ArrayList<>();
    for (ExprNodeDesc key : topNKeyDesc.getKeyColumns()) {
      final String keyString = key.getExprString();
      if (key instanceof ExprNodeConstantDesc) {
        mappedKeyColumns.add(key);
      } else if (selectMap.containsKey(keyString)) {
        mappedKeyColumns.add(selectMap.get(keyString));
      } else {
        return;
      }
    }

    final TopNKeyDesc newTopNKeyDesc = new TopNKeyDesc(topNKeyDesc.getTopN(),
        topNKeyDesc.getColumnSortOrder(), mappedKeyColumns);
    selectOperator.removeChildAndAdoptItsChildren(topNKeyOperator);
    pushdown(createOperatorBetween(selectOperator.getParentOperators().get(0), selectOperator,
        newTopNKeyDesc));
  }

  /**
   * Push through GroupBy. No grouping sets. If TopNKey expression is same as GroupBy expression,
   * we can push it and remove it from above GroupBy. If expression in TopNKey shared common prefix
   * with GroupBy, TopNKey could be pushed through GroupBy using that prefix and kept above it.
   * @param topNKeyOperator
   * @return
   */
  private TopNKeyOperator pushdownThroughGroupBy(TopNKeyOperator topNKeyOperator) {
    final GroupByOperator groupByOperator =
        (GroupByOperator) topNKeyOperator.getParentOperators().get(0);

    LOG.info("a");

    // No grouping sets
    if (groupByOperator.getConf().isGroupingSetsPresent()) {
      LOG.info("b");
      return null;
    }

    LOG.info("t.k: " + topNKeyOperator.getConf().getKeyColumns());
    LOG.info("t.m: " + topNKeyOperator.getColumnExprMap());
    LOG.info("g.k: " + groupByOperator.getConf().getKeys());
    LOG.info("g.m: " + groupByOperator.getColumnExprMap());

    // If TopNKey expression is same as GroupBy expression
    if (ExprNodeDescUtils.isSame(topNKeyOperator.getConf().getKeyColumns(), groupByOperator.getConf().getKeys())) {
      return null;
    }

    // We can push it and remove it from above GroupBy.
    return null;
  }

  /**
   * Push through ReduceSink. If TopNKey expression is same as ReduceSink expression and order is
   * the same, we can push it and remove it from above ReduceSink. If expression in TopNKey shared
   * common prefix with ReduceSink including same order, TopNKey could be pushed through ReduceSink
   * using that prefix and kept above it.
   * @param currentOperator
   * @return
   */
  private TopNKeyOperator pushdownThroughReduceSink(TopNKeyOperator currentOperator) {
    final Operator<? extends OperatorDesc> parentOperator =
        currentOperator.getParentOperators().get(0);
    return null;
  }

  private TopNKeyOperator pushdownThroughFullOuterJoin(TopNKeyOperator currentOperator) {
    final Operator<? extends OperatorDesc> parentOperator =
        currentOperator.getParentOperators().get(0);
    /*
     Push through FOJ. Push TopNKey expression without keys to largest input. Keep on top of FOJ.
     */
    return null;
  }

  private TopNKeyOperator pushdownThroughLeftOuterJoin(TopNKeyOperator currentOperator) {
    final Operator<? extends OperatorDesc> parentOperator =
        currentOperator.getParentOperators().get(0);
    /*
     Push through LOJ. If TopNKey expression refers fully to expressions from left input, push with
     rewriting of expressions and remove from top of LOJ. If TopNKey expression has a prefix that
     refers to expressions from left input, push with rewriting of those expressions and keep on
     top of LOJ.
     */
    return null;
  }

  private TopNKeyOperator pushdownThroughRightOuterJoin(TopNKeyOperator currentOperator) {
    final Operator<? extends OperatorDesc> parentOperator =
        currentOperator.getParentOperators().get(0);
    return null;
  }
}
