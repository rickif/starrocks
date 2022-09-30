// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

package com.starrocks.load.routineload;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.starrocks.common.FeConstants;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.UserException;
import com.starrocks.common.util.LeaderDaemon;
import com.starrocks.common.util.LogBuilder;
import com.starrocks.common.util.LogKey;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

public class RoutineLoadPipelinedScheduler extends LeaderDaemon {

    private static final Logger LOG = LogManager.getLogger(RoutineLoadPipelinedScheduler.class);

    private RoutineLoadManager routineLoadManager;

    @VisibleForTesting
    public RoutineLoadPipelinedScheduler() {
        super();
        routineLoadManager = GlobalStateMgr.getCurrentState().getRoutineLoadManager();
    }

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
            RoutineLoadJob.JobState errorJobState = null;
            UserException userException = null;
            try {
                routineLoadJob.prepare();
                // judge nums of tasks more than max concurrent tasks of cluster
                int desiredConcurrentTaskNum = routineLoadJob.calculateCurrentConcurrentTaskNum();
                if (desiredConcurrentTaskNum <= 0) {
                    // the job will be rescheduled later.
                    LOG.info(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                            .add("msg", "the current concurrent num is less then or equal to zero, "
                                    + "job will be rescheduled later")
                            .build());
                    continue;
                }
                // check state and divide job into tasks
                routineLoadJob.divideRoutineLoadJob(desiredConcurrentTaskNum);
            } catch (MetaNotFoundException e) {
                errorJobState = RoutineLoadJob.JobState.CANCELLED;
                userException = e;
                LOG.warn(userException.getMessage());
            } catch (UserException e) {
                errorJobState = RoutineLoadJob.JobState.PAUSED;
                userException = e;
                LOG.warn(userException.getMessage());
            }

            if (errorJobState != null) {
                LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                        .add("current_state", routineLoadJob.getState())
                        .add("desired_state", errorJobState)
                        .add("warn_msg",
                                "failed to scheduler job, change job state to desired_state with error reason " +
                                        userException.getMessage())
                        .build(), userException);
                try {
                    ErrorReason reason = new ErrorReason(userException.getErrorCode(), userException.getMessage());
                    routineLoadJob.updateState(errorJobState, reason, false);
                } catch (UserException e) {
                    LOG.warn(new LogBuilder(LogKey.ROUTINE_LOAD_JOB, routineLoadJob.getId())
                            .add("current_state", routineLoadJob.getState())
                            .add("desired_state", errorJobState)
                            .add("warn_msg", "failed to change state to desired state")
                            .build(), e);
                }
            }
        }

        // check timeout tasks
        routineLoadManager.processTimeoutTasks();
    }

    private List<RoutineLoadJob> getNeedScheduleRoutineJobs() {
        return routineLoadManager.getRoutineLoadJobByState(Sets.newHashSet(RoutineLoadJob.JobState.NEED_SCHEDULE));
    }
}
