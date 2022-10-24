// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.load.routineload;

import com.google.common.collect.Sets;
import com.starrocks.common.FeConstants;
import com.starrocks.common.UserException;
import com.starrocks.common.util.LeaderDaemon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RoutineLoadPipelinedScheduler extends LeaderDaemon {

    private static final Logger LOG = LogManager.getLogger(RoutineLoadPipelinedScheduler.class);

    private RoutineLoadManager routineLoadManager;

    private static final int THREAD_POOL_SIZE = 10;
    private final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

    public RoutineLoadPipelinedScheduler(RoutineLoadManager routineLoadManager) {
        super("Routine load scheduler", FeConstants.default_scheduler_interval_millisecond);
        this.routineLoadManager = routineLoadManager;
    }

    @Override
    protected void runAfterCatalogReady() {
        try {
            process();
        } catch (Throwable e) {
            LOG.warn("Failed to process one round of RoutineLoadScheduler", e);
        }
    }

    private void process() throws UserException {
        // update
        routineLoadManager.updateRoutineLoadJob();
        // get need schedule routine jobs
        List<RoutineLoadJob> routineLoadJobList = getNeedScheduleRoutineJobs();

        LOG.info("there are {} job need schedule", routineLoadJobList.size());
        for (RoutineLoadJob routineLoadJob : routineLoadJobList) {
            threadPool.submit(() -> {
                try {
                    routineLoadJob.execute();
                } catch (UserException e) {
                    LOG.warn("routine load job execute: ", e.getMessage());
                }
            });
        }
    }

    private List<RoutineLoadJob> getNeedScheduleRoutineJobs() {
        return routineLoadManager.getRoutineLoadJobByState(Sets.newHashSet(RoutineLoadJob.JobState.NEED_SCHEDULE));
    }
}
