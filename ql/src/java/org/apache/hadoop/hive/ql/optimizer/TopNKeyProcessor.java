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

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TopNKeyDesc;

import java.util.List;
import java.util.Stack;

public class TopNKeyProcessor implements NodeProcessor {

  public TopNKeyProcessor() {
  }

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
      Object... nodeOutputs) throws SemanticException {

    // Get RS
    ReduceSinkOperator reduceSinkOperator = (ReduceSinkOperator) nd;
    ReduceSinkDesc reduceSinkDesc = reduceSinkOperator.getConf();

    // It only allows an ordered and limited reduce sink
    if (!reduceSinkDesc.isOrdering() || reduceSinkDesc.getTopN() < 0) {
      return null;
    }

    // Get columns and column names
    List<ExprNodeDesc> keyColumns = reduceSinkDesc.getKeyCols();

    // Get the parent operator and the current operator
    int depth = stack.size() - 1;
    Operator<? extends OperatorDesc> parentOperator =
        (Operator<? extends OperatorDesc>) stack.get(depth - 1);
    Operator<? extends OperatorDesc> currentOperator =
        (Operator<? extends OperatorDesc>) stack.get(depth);

    // Insert a new top n key operator between the parent operator and the current operator
    TopNKeyDesc topNKeyDesc = new TopNKeyDesc(reduceSinkDesc.getTopN(), reduceSinkDesc.getOrder(),
        keyColumns);
    makeOpBetween(topNKeyDesc, new RowSchema(reduceSinkOperator.getSchema()), parentOperator,
        currentOperator);

    return null;
  }

  private static void makeOpBetween(OperatorDesc newOperatorDesc, RowSchema rowSchema,
      Operator<? extends OperatorDesc> parentOperator,
      Operator<? extends OperatorDesc> currentOperator) {

    // PARENT -> (NEW, (CURRENT -> CHILD))
    Operator<? extends OperatorDesc> newOperator = OperatorFactory.getAndMakeChild(
        currentOperator.getCompilationOpContext(), newOperatorDesc, rowSchema,
        currentOperator.getParentOperators());

    // PARENT -> NEW -> CURRENT -> CHILD
    newOperator.getChildOperators().add(currentOperator);
    currentOperator.getParentOperators().add(newOperator);
    parentOperator.removeChild(currentOperator);
  }
}
