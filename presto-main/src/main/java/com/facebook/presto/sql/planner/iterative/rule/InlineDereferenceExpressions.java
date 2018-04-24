/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.BaseExtractor;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;

public class InlineDereferenceExpressions
        implements Rule<ProjectNode>
{
    private static final BaseExtractor BASE_EXTRACTOR = new BaseExtractor();
    private static final Capture<ProjectNode> CHILD = newCapture();

    private static final Pattern<ProjectNode> PATTERN = project()
            .with(source().matching(project().capturedAs(CHILD)));

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return SystemSessionProperties.isDereferenceExpressionPushDown(session);
    }

    @Override
    public Result apply(ProjectNode parent, Captures captures, Context context)
    {
        ProjectNode child = captures.get(CHILD);
        Set<Symbol> childSymbols = child.getAssignments().getSymbols();
        Function<Expression, Boolean> isBaseSymbolInChild = expression -> expression instanceof DereferenceExpression && childSymbols.contains(BASE_EXTRACTOR.process(expression));
        Set<Symbol> dereferenceSymbols = parent.getAssignments().entrySet().stream().filter(entry -> isBaseSymbolInChild.apply(entry.getValue())).map(Map.Entry::getKey).collect(Collectors.toSet());
        if (dereferenceSymbols.isEmpty()) {
            return Result.empty();
        }

        Assignments newParentAssignments = Assignments.builder().putAll(parent.getAssignments().filter(symbol -> !dereferenceSymbols.contains(symbol))).putIdentities(dereferenceSymbols).build();
        Assignments newChildAssignments = Assignments.builder().putAll(child.getAssignments()).putAll(parent.getAssignments().filter(dereferenceSymbols::contains)).build();
        return Result.ofPlanNode(
                new ProjectNode(
                        parent.getId(),
                        new ProjectNode(
                                child.getId(),
                                child.getSource(),
                                newChildAssignments),
                        newParentAssignments));
    }
}
