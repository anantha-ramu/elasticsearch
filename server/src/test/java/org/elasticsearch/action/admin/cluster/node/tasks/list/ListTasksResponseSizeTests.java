/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.tasks.list;

import org.apache.lucene.tests.util.RamUsageTester;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.cluster.node.tasks.TaskManagerTestCase;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;

/**
 * How large a {@code GET /_tasks} response gets is decided by how busy the cluster happens to be, not by anything in the
 * request. Every matching task is turned into a {@link TaskInfo} and held on the coordinating node until every node has
 * reported, and nothing is written until then, so the request circuit breaker never sees any of it.
 *
 * <p>On a serverless search node that ran out of memory, one such call built 586,403 {@link TaskInfo} objects retaining
 * 564MB of a 992MB heap. The caller was Kibana's periodic unfiltered poll. Nothing about the request was unusual — an
 * indexing node it fanned out to happened to have 14k bulk tasks in flight, and the count did the rest.
 */
public class ListTasksResponseSizeTests extends TaskManagerTestCase {

    private static final String TEST_ACTION = "internal:test/list-tasks-size";

    /**
     * A ceiling one management call should not go past. Deliberately generous: the point is that some bound exists, not
     * that this is the right figure, which is a decision for whoever reviews this.
     */
    private static final long RESPONSE_BUDGET_BYTES = 16 * 1024 * 1024;

    /** Tasks collected by the single {@code GET /_tasks} call on the node that ran out of memory. */
    private static final int OBSERVED_TASK_COUNT = 586_403;

    public void testTooManyTasksFailsInsteadOfFillingTheHeap() throws Exception {
        final int maxTasks = 20;
        final TestNode coordinator = startCluster(maxTasks);
        final List<Task> tasks = registerTasks(coordinator, maxTasks + 5);
        try {
            final ElasticsearchStatusException e = safeAwaitAndUnwrapFailure(
                ElasticsearchStatusException.class,
                ListTasksResponse.class,
                listener -> ActionTestUtils.execute(coordinator.transportListTasksAction, listTasksRequest(), listener)
            );

            assertThat(e.status(), equalTo(RestStatus.TOO_MANY_REQUESTS));
            assertThat(e.getMessage(), containsString("matched [" + tasks.size() + "] tasks"));
            assertThat(e.getMessage(), containsString("more than the [" + maxTasks + "]"));
        } finally {
            unregister(coordinator, tasks);
        }
    }

    public void testTasksUpToTheLimitAreStillReturned() throws Exception {
        final int maxTasks = 20;
        final TestNode coordinator = startCluster(maxTasks);
        final List<Task> tasks = registerTasks(coordinator, maxTasks);
        try {
            final ListTasksResponse response = ActionTestUtils.executeBlocking(coordinator.transportListTasksAction, listTasksRequest());

            assertThat(response.getTasks(), hasSize(tasks.size()));
        } finally {
            unregister(coordinator, tasks);
        }
    }

    /**
     * Guards the default: a limit only helps if the response it permits actually fits. If someone raises the default,
     * this is where they find out what they have signed up for.
     */
    public void testTheDefaultLimitKeepsTheResponseWithinBudget() {
        final int sample = 10_000;
        final long perTask = RamUsageTester.ramUsed(sampleTasks(sample)) / sample;
        final int defaultMaxTasks = TransportListTasksAction.MAX_LISTED_TASKS_SETTING.get(Settings.EMPTY);

        logger.info(
            "{} bytes per task: the default limit of {} tasks retains {} bytes, the {} tasks seen in the incident would "
                + "have retained {}",
            perTask,
            defaultMaxTasks,
            perTask * defaultMaxTasks,
            OBSERVED_TASK_COUNT,
            perTask * OBSERVED_TASK_COUNT
        );

        // These TaskInfo objects are simpler than the ones in the incident, which retained around 962 bytes each, so
        // this understates the real cost.
        assertThat(perTask * defaultMaxTasks, lessThan(RESPONSE_BUDGET_BYTES));
    }

    private TestNode startCluster(int maxTasks) {
        setupTestNodes(Settings.EMPTY);
        connectNodes(testNodes);
        final TestNode coordinator = testNodes[0];
        coordinator.clusterService.getClusterSettings()
            .applySettings(Settings.builder().put(TransportListTasksAction.MAX_LISTED_TASKS_SETTING.getKey(), maxTasks).build());
        return coordinator;
    }

    /** Matches only the tasks this test registers, which also keeps us off the double-listing path used for reindex. */
    private static ListTasksRequest listTasksRequest() {
        return new ListTasksRequest().setActions(TEST_ACTION);
    }

    private static List<Task> registerTasks(TestNode node, int count) {
        final TaskManager taskManager = node.transportService.getTaskManager();
        final List<Task> tasks = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            tasks.add(taskManager.register("transport", TEST_ACTION, new TaskAwareRequest() {
                @Override
                public void setParentTask(TaskId taskId) {}

                @Override
                public void setRequestId(long requestId) {}

                @Override
                public TaskId getParentTask() {
                    return TaskId.EMPTY_TASK_ID;
                }
            }));
        }
        return tasks;
    }

    private static void unregister(TestNode node, List<Task> tasks) {
        final TaskManager taskManager = node.transportService.getTaskManager();
        for (Task task : tasks) {
            taskManager.unregister(task);
        }
    }

    private static List<TaskInfo> sampleTasks(int count) {
        final List<TaskInfo> tasks = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            tasks.add(
                new TaskInfo(
                    new TaskId("node-1", i),
                    "transport",
                    "node-1",
                    "indices:data/write/bulk[s]",
                    "requests[1], indices[an-index-with-a-realistic-name]",
                    null,
                    System.nanoTime(),
                    0L,
                    true,
                    false,
                    TaskId.EMPTY_TASK_ID,
                    Map.of("X-Opaque-Id", "kibana-" + i),
                    new TaskId("node-1", i),
                    System.currentTimeMillis()
                )
            );
        }
        return tasks;
    }
}
