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
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.TopNKeyOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import static org.apache.hadoop.hive.ql.optimizer.TopNKeyProcessor.columnsByNames;
import static org.apache.hadoop.hive.ql.optimizer.TopNKeyProcessor.createOperatorBetween;
import static org.apache.hadoop.hive.ql.plan.ExprNodeDesc.ExprNodeDescEqualityWrapper.transform;

public class TopNKeyPushdownProcessor implements NodeProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(TopNKeyPushdownProcessor.class);

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
      Object... nodeOutputs) throws SemanticException {
    TopNKeyOperator currentOperator = (TopNKeyOperator) nd;
    pushdown(currentOperator);
    return null;
  }

  private TopNKeyOperator pushdown(TopNKeyOperator currentOperator) {
    Operator<? extends OperatorDesc> parentOperator = currentOperator.getParentOperators().get(0);

    switch (parentOperator.getType()) {
      case LIMIT:
      case FILTER:
      case SELECT:
      case SCRIPT:
      case FORWARD:
        return pushdownThroughProject(currentOperator);

      case GROUPBY:
        return pushdownThroughGroupBy(currentOperator);

      case REDUCESINK:
        return pushdownThroughReduceSink(currentOperator);

      case MAPJOIN:
      case MERGEJOIN:
      case JOIN:
        JoinOperator joinOperator = (JoinOperator) parentOperator;
        JoinCondDesc[] joinConds = joinOperator.getConf().getConds();
        if (joinConds.length == 1) {
          switch (joinConds[0].getType()) {
            case JoinDesc.FULL_OUTER_JOIN:
              return pushdownThroughFullOuterJoin(currentOperator);
            case JoinDesc.LEFT_OUTER_JOIN:
              return pushdownThroughLeftOuterJoin(currentOperator);
            case JoinDesc.RIGHT_OUTER_JOIN:
              return pushdownThroughRightOuterJoin(currentOperator);
          }
        }
    }
    return null;
  }

  private TopNKeyOperator pushdownThroughProject(TopNKeyOperator currentOperator) {
    final Operator<? extends OperatorDesc> parentOperator = getSingleParent(currentOperator);

    // Check whether TopNKey key columns can be mapped to expressions based on Project input
    final Map<String, ExprNodeDesc> columnExprMap = parentOperator.getColumnExprMap();
    if (columnExprMap == null) {
      return null;
    }
    final Collection<ExprNodeDesc> projectInputs = columnExprMap.values();
    final TopNKeyDesc currentDesc = currentOperator.getConf();
    if (!transform(projectInputs).containsAll(transform(currentDesc.getKeyColumns()))) {
      return null;
    }

    final TopNKeyDesc topNKeyDesc = new TopNKeyDesc(currentDesc.getTopN(),
        currentDesc.getColumnSortOrder(), currentDesc.getKeyColumns());
    removeOperatorAndConnectParentAndChild(currentOperator);
    final Operator<? extends OperatorDesc> grandParentOperator = getSingleParent(parentOperator);
    return (TopNKeyOperator) createOperatorBetween(grandParentOperator, parentOperator, topNKeyDesc);
  }

  private void removeOperatorAndConnectParentAndChild(Operator<? extends OperatorDesc> operator) {
    Operator<? extends OperatorDesc> parent = getSingleParent(operator);
    Operator<? extends OperatorDesc> child = getSingleChild(operator);
    parent.replaceChild(operator, child);
    child.replaceParent(operator, parent);
  }

  private Operator<? extends OperatorDesc> getSingleParent(
      Operator<? extends OperatorDesc> operator) {
    List<Operator<? extends OperatorDesc>> parents = operator.getParentOperators();
    if (parents.size() != 1) {
      return null;
    }
    return parents.get(0);
  }

  private Operator<? extends OperatorDesc> getSingleChild(
      Operator<? extends OperatorDesc> operator) {
    List<Operator<? extends OperatorDesc>> children = operator.getChildOperators();
    if (children.size() != 1) {
      return null;
    }
    return children.get(0);
  }

  private TopNKeyOperator pushdownThroughGroupBy(TopNKeyOperator currentOperator) {
    final GroupByOperator groupByOperator =
        (GroupByOperator) currentOperator.getParentOperators().get(0);

    // Check whether GroupBy has grouping sets
    if (groupByOperator.getConf().isGroupingSetsPresent()) {
      return null;
    }

    /*
     Push through GroupBy. No grouping sets. If TopNKey expression is same as GroupBy expression,
     we can push it and remove it from above GroupBy. If expression in TopNKey shared common prefix
     with GroupBy, TopNKey could be pushed through GroupBy using that prefix and kept above it.
     */

    return null;
  }

  private TopNKeyOperator pushdownThroughReduceSink(TopNKeyOperator currentOperator) {
    final Operator<? extends OperatorDesc> parentOperator =
        currentOperator.getParentOperators().get(0);
    /*
     Push through ReduceSink. If TopNKey expression is same as ReduceSink expression and order is
     the same, we can push it and remove it from above ReduceSink. If expression in TopNKey shared
     common prefix with ReduceSink including same order, TopNKey could be pushed through ReduceSink
     using that prefix and kept above it.
     */
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
