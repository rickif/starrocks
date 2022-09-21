// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.alter.SchemaChangeHandler;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.analysis.DescriptorTable;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.ImportColumnDesc;
import com.starrocks.analysis.SlotDescriptor;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.StringLiteral;
import com.starrocks.analysis.TableName;
import com.starrocks.analysis.TupleDescriptor;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.IdGenerator;
import com.starrocks.common.LoadException;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.DebugUtil;
import com.starrocks.load.BrokerFileGroup;
import com.starrocks.load.EtlJobType;
import com.starrocks.load.Load;
import com.starrocks.planner.DataPartition;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.ExchangeNode;
import com.starrocks.planner.FileScanNode;
import com.starrocks.planner.MysqlTableSink;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.planner.PlanFragment;
import com.starrocks.planner.PlanFragmentId;
import com.starrocks.planner.PlanNodeId;
import com.starrocks.planner.ScanNode;
import com.starrocks.planner.StreamLoadScanNode;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.analyzer.AnalyzeState;
import com.starrocks.sql.analyzer.ExpressionAnalyzer;
import com.starrocks.sql.analyzer.Field;
import com.starrocks.sql.analyzer.RelationFields;
import com.starrocks.sql.analyzer.RelationId;
import com.starrocks.sql.analyzer.Scope;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.CreateMaterializedViewStmt;
import com.starrocks.sql.ast.DefaultValueExpr;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.PartitionNames;
import com.starrocks.sql.ast.QueryRelation;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.sql.common.TypeManager;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Optimizer;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.base.DistributionProperty;
import com.starrocks.sql.optimizer.base.DistributionSpec;
import com.starrocks.sql.optimizer.base.GatherDistributionSpec;
import com.starrocks.sql.optimizer.base.HashDistributionDesc;
import com.starrocks.sql.optimizer.base.PhysicalPropertySet;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.optimizer.rewrite.scalar.FoldConstantsRule;
import com.starrocks.sql.optimizer.rewrite.scalar.ScalarOperatorRewriteRule;
import com.starrocks.sql.optimizer.statistics.ColumnDict;
import com.starrocks.sql.optimizer.statistics.IDictManager;
import com.starrocks.sql.optimizer.transformer.ExpressionMapping;
import com.starrocks.sql.optimizer.transformer.LogicalPlan;
import com.starrocks.sql.optimizer.transformer.OptExprBuilder;
import com.starrocks.sql.optimizer.transformer.RelationTransformer;
import com.starrocks.sql.optimizer.transformer.SqlToScalarOperatorTranslator;
import com.starrocks.sql.plan.ExecPlan;
import com.starrocks.sql.plan.PlanFragmentBuilder;
import com.starrocks.task.StreamLoadTask;
import com.starrocks.thrift.TBrokerFileStatus;
import com.starrocks.thrift.TPartitionType;
import com.starrocks.thrift.TResultSinkType;
import com.starrocks.thrift.TRoutineLoadTask;
import com.starrocks.thrift.TUniqueId;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.starrocks.catalog.DefaultExpr.SUPPORTED_DEFAULT_FNS;

public class InsertPlannerV2 {
    private static Logger LOG = LogManager.getLogger(InsertPlannerV2.class);
    // Only for unit test
    public static boolean enableSingleReplicationShuffle = false;
    
    private long loadJobId;
    private TUniqueId loadId;
    private long txnId;
    private ConnectContext context;
    private EtlJobType etlJobType;
    private String timezone;
    private boolean partialUpdate;
    boolean isPrimaryKey;
    boolean enableDictOptimize;
    private long execMemLimit;
    private long loadMemLimit;
    private boolean strictMode;
    private long timeoutS;
    private int parallelInstanceNum;
    private long startTime;
    private long sqlMode;
    private String loadTransmissionCompressionType;
    private boolean enableReplicatedSorage;
    private Map<String, String> sessionVariables;

    private long dbId;
    private Table destTable;
    private DescriptorTable descTable;
    private TupleDescriptor tupleDesc;
    private List<Pair<Integer, ColumnDict>> globalDicts = Lists.newArrayList();

    private List<PlanFragment> fragments = Lists.newArrayList();
    private List<ScanNode> scanNodes = Lists.newArrayList();

    // Broker Load related structs
    private BrokerDesc brokerDesc;
    private List<BrokerFileGroup> fileGroups;
    private List<List<TBrokerFileStatus>> fileStatusesList;
    private int filesAdded;
    private Analyzer analyzer;

    IdGenerator<PlanNodeId> planNodeGenerator = PlanNodeId.createGenerator();

    // Insert into related structs
    private InsertStmt insertStmt;

    // Routine/Stream load related structs
    List<ImportColumnDesc> columnDescs;
    private StreamLoadTask streamLoadTask;
    boolean routimeStreamLoadNegative;

    // Routine load related structs
    TRoutineLoadTask routineLoadTask;

    public InsertPlannerV2(long loadJobId, TUniqueId loadId, long txnId, long dbId, OlapTable destTable,
                            boolean strictMode, String timezone, long timeoutS,
                            long startTime, boolean partialUpdate, ConnectContext context,
                            Map<String, String> sessionVariables, long loadMemLimit, long execMemLimit, 
                            BrokerDesc brokerDesc, List<BrokerFileGroup> brokerFileGroups, 
                            List<List<TBrokerFileStatus>> fileStatusesList, int filesAdded) {
        this.loadJobId = loadJobId;
        this.loadId = loadId;
        this.txnId = txnId;
        this.dbId = dbId;
        this.destTable = destTable;
        this.strictMode = strictMode;
        this.timeoutS = timeoutS;
        this.partialUpdate = partialUpdate;
        this.parallelInstanceNum = Config.load_parallel_instance_num;
        this.startTime = startTime;
        if (this.context != null) {
            this.context = context;
        } else {
            this.context = new ConnectContext();
        }
        this.analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), this.context);
        this.analyzer.setTimezone(timezone);
        this.timezone = timezone;
        this.descTable = this.analyzer.getDescTbl();
        this.loadMemLimit = loadMemLimit;
        this.execMemLimit = execMemLimit;
        this.sessionVariables = sessionVariables;
        this.brokerDesc = brokerDesc;
        this.fileGroups = brokerFileGroups;
        this.fileStatusesList = fileStatusesList;
        this.filesAdded = filesAdded;
        this.isPrimaryKey = ((OlapTable) destTable).getKeysType() == KeysType.PRIMARY_KEYS;
        this.enableDictOptimize = true;
        this.routimeStreamLoadNegative = false;
        this.etlJobType = EtlJobType.BROKER;
    }
    
    public InsertPlannerV2(InsertStmt insertStmt, ConnectContext context) {
        this.insertStmt = insertStmt;
        this.context = context;
        this.enableDictOptimize = true;
        this.execMemLimit = context.getSessionVariable().getMaxExecMemByte();
        this.loadMemLimit = context.getSessionVariable().getLoadMemLimit();
        this.routimeStreamLoadNegative = false;
        this.etlJobType = EtlJobType.INSERT;
    }

    public InsertPlannerV2(long loadJobId, TUniqueId loadId, long txnId, long dbId, OlapTable destTable,
            boolean strictMode, String timezone,  boolean partialUpdate, ConnectContext context,
            Map<String, String> sessionVariables, long loadMemLimit, long execMemLimit, boolean routimeStreamLoadNegative, 
            List<ImportColumnDesc> columnDescs, StreamLoadTask streamLoadTask, TRoutineLoadTask routineLoadTask) {
        this.loadJobId = loadJobId;
        this.loadId = loadId;
        this.txnId = txnId;
        this.dbId = dbId;
        this.destTable = destTable;
        this.strictMode = strictMode;
        this.timezone = timezone;
        this.partialUpdate = partialUpdate;
        if (context != null) {
            this.context = context;
        } else {
            this.context = new ConnectContext();
        }
        this.loadMemLimit = loadMemLimit;
        this.execMemLimit = execMemLimit;
        this.isPrimaryKey = ((OlapTable) destTable).getKeysType() == KeysType.PRIMARY_KEYS;
        this.routimeStreamLoadNegative = routimeStreamLoadNegative;
        this.columnDescs = columnDescs;
        this.streamLoadTask = streamLoadTask;
        this.analyzer = new Analyzer(GlobalStateMgr.getCurrentState(), this.context);
        this.descTable = analyzer.getDescTbl();
        this.enableDictOptimize = Config.enable_dict_optimize_routine_load;
        this.timeoutS = Config.routine_load_task_timeout_second;
        this.startTime = System.currentTimeMillis();
        this.sessionVariables = sessionVariables;
        this.etlJobType = EtlJobType.ROUTINE_LOAD;
        this.routineLoadTask = routineLoadTask;
    }

    public ExecPlan plan() throws UserException {
        if (this.etlJobType == EtlJobType.BROKER) {
            broker_load_plan();
            return null;
        } else if (this.etlJobType == EtlJobType.INSERT) {
            return insert_into_plan();
        } else if (this.etlJobType == EtlJobType.ROUTINE_LOAD || this.etlJobType == EtlJobType.STREAM_LOAD) {
            routine_stream_load_plan();
            return null;
        }
        return null;
    }

    public void broker_load_plan() throws UserException {
        // 1. Generate tuple descriptor
        OlapTable olapDestTable = (OlapTable) destTable;
        List<Column> destColumns = Lists.newArrayList();
        if (isPrimaryKey && partialUpdate) {
            if (fileGroups.size() > 1) {
                throw new DdlException("partial update only support single filegroup.");
            } else if (fileGroups.size() == 1) {
                if (fileGroups.get(0).isNegative()) {
                    throw new DdlException("Primary key table does not support negative load");
                }
                destColumns = Load.getPartialUpateColumns(destTable, fileGroups.get(0).getColumnExprList());
            } else {
                throw new DdlException("filegroup number=" + fileGroups.size() + " is illegal");
            }
        } else if (!isPrimaryKey && partialUpdate) {
            throw new DdlException("Only primary key table support partial update");
        } else {
            destColumns = destTable.getFullSchema();
        }
        generateTupleDescriptor(destColumns, isPrimaryKey);

        // 2. Prepare scan nodes
        ScanNode scanNode = prepareScanNodes();

        // 3. Exchange node for primary table
        PlanFragment sinkFragment = null;
        boolean needShufflePlan = false;
        if (Config.enable_shuffle_load && needShufflePlan()) {

            // scan fragment
            PlanFragment scanFragment = new PlanFragment(new PlanFragmentId(0), scanNode, DataPartition.RANDOM);
            scanFragment.setParallelExecNum(parallelInstanceNum);

            fragments.add(scanFragment);

            // Exchange node
            List<Column> keyColumns = olapDestTable.getKeyColumnsByIndexId(olapDestTable.getBaseIndexId());
            List<Expr> partitionExprs = Lists.newArrayList();
            keyColumns.forEach(column -> {
                partitionExprs.add(new SlotRef(tupleDesc.getColumnSlot(column.getName())));
            });

            DataPartition dataPartition = new DataPartition(TPartitionType.HASH_PARTITIONED, partitionExprs); 
            ExchangeNode exchangeNode = new ExchangeNode(new PlanNodeId(planNodeGenerator.getNextId().asInt()), 
                    scanFragment.getPlanRoot(), dataPartition);
            
            // add exchange node to scan fragment and sink fragment
            sinkFragment = new PlanFragment(new PlanFragmentId(1), exchangeNode, dataPartition);
            exchangeNode.setFragment(sinkFragment);
            scanFragment.setDestination(exchangeNode);
            scanFragment.setOutputPartition(dataPartition);

            needShufflePlan = true;
        }

        // 4. Prepare sink fragment
        List<Long> partitionIds = getAllPartitionIds();
        if (!needShufflePlan) {
            sinkFragment = new PlanFragment(new PlanFragmentId(0), scanNode, DataPartition.RANDOM);
        }
        // Parallel pipeline for broker loads are currently not supported, so disable the pipeline engine when users need parallel load
        prepareSinkFragment(sinkFragment, partitionIds, parallelInstanceNum <= 1, true);
        fragments.add(sinkFragment);

        // 5. finalize
        for (PlanFragment fragment : fragments) {
            fragment.createDataSink(TResultSinkType.MYSQL_PROTOCAL);
        }
        Collections.reverse(fragments);
    }

    public ExecPlan insert_into_plan() throws UserException {
        QueryRelation queryRelation = insertStmt.getQueryStatement().getQueryRelation();
        List<ColumnRefOperator> outputColumns = new ArrayList<>();

        //1. Process the literal value of the insert values type and cast it into the type of the target table
        if (queryRelation instanceof ValuesRelation) {
            castLiteralToTargetColumnsType(insertStmt);
        }

        //2. Build Logical plan
        ColumnRefFactory columnRefFactory = new ColumnRefFactory();
        LogicalPlan logicalPlan =
                new RelationTransformer(columnRefFactory, context).transform(
                        insertStmt.getQueryStatement().getQueryRelation());

        //3. Fill in the default value and NULL
        OptExprBuilder optExprBuilder = fillDefaultValue(logicalPlan, columnRefFactory, insertStmt, outputColumns);

        //4. Fill in the shadow column
        optExprBuilder = fillShadowColumns(columnRefFactory, insertStmt, outputColumns, optExprBuilder, context);

        //5. Cast output columns type to target type
        optExprBuilder =
                castOutputColumnsTypeToTargetColumns(columnRefFactory, insertStmt, outputColumns, optExprBuilder);

        //6. Optimize logical plan and build physical plan
        logicalPlan = new LogicalPlan(optExprBuilder, outputColumns, logicalPlan.getCorrelation());

        // TODO: remove forceDisablePipeline when all the operators support pipeline engine.
        boolean isEnablePipeline = context.getSessionVariable().isEnablePipelineEngine();
        // Parallel pipeline loads are currently not supported, so disable the pipeline engine when users need parallel load
        boolean canUsePipeline =
                isEnablePipeline && DataSink.canTableSinkUsePipeline(insertStmt.getTargetTable()) &&
                        logicalPlan.canUsePipeline() && context.getSessionVariable().getParallelExecInstanceNum() <= 1;
        boolean forceDisablePipeline = isEnablePipeline && !canUsePipeline;

        try {
            if (forceDisablePipeline) {
                context.getSessionVariable().setEnablePipelineEngine(false);
            }
            Optimizer optimizer = new Optimizer();
            PhysicalPropertySet requiredPropertySet = createPhysicalPropertySet(insertStmt, outputColumns);
            OptExpression optimizedPlan = optimizer.optimize(
                    context,
                    logicalPlan.getRoot(),
                    requiredPropertySet,
                    new ColumnRefSet(logicalPlan.getOutputColumn()),
                    columnRefFactory);

            //7. Build fragment exec plan
            boolean hasOutputFragment = ((queryRelation instanceof SelectRelation && queryRelation.hasLimit())
                    || insertStmt.getTargetTable() instanceof MysqlTable);
            ExecPlan execPlan = new PlanFragmentBuilder().createPhysicalPlan(
                    optimizedPlan, context, logicalPlan.getOutputColumn(), columnRefFactory,
                    queryRelation.getColumnOutputNames(), TResultSinkType.MYSQL_PROTOCAL, hasOutputFragment);

            this.strictMode = context.getSessionVariable().getEnableInsertStrict();
            this.timeoutS = context.getSessionVariable().getQueryTimeoutS();
            this.loadId = context.getExecutionId();
            this.timezone = context.getSessionVariable().getTimeZone();
            this.partialUpdate = false;
            this.dbId = MetaUtils.getDatabase(context, insertStmt.getTableName()).getId();
            this.descTable = execPlan.getDescTbl();
            this.destTable = insertStmt.getTargetTable();

            //8. Generate tuple descriptor
            generateTupleDescriptor(insertStmt.getTargetTable().getFullSchema(), false);

            //9. Prepare sink fragment
            PlanFragment sinkFragment = execPlan.getFragments().get(0);
            prepareSinkFragment(sinkFragment, insertStmt.getTargetPartitionIds(), canUsePipeline, false);

            execPlan.setInsertPlanner(this);

            this.fragments = execPlan.getFragments();
            this.scanNodes = execPlan.getScanNodes();
            return execPlan;
        } finally {
            if (forceDisablePipeline) {
                context.getSessionVariable().setEnablePipelineEngine(true);
            }
        }

    }

    public void routine_stream_load_plan() throws UserException {
        OlapTable olapDestTable = (OlapTable) destTable;
        // 1. Generate tuple descriptor
        if (isPrimaryKey) {
            if (routimeStreamLoadNegative) {
                throw new DdlException("Primary key table does not support negative load");
            }
        } else {
            if (partialUpdate) {
                throw new DdlException("Only primary key table support partial update");
            }
        }
        List<Column> destColumns = null;
        if (partialUpdate) {
            destColumns = Load.getPartialUpateColumns(destTable, columnDescs);
        } else {
            destColumns = destTable.getFullSchema();
        }
        generateTupleDescriptor(destColumns, isPrimaryKey);

        // 2. Prepare scan nodes
        ScanNode scanNode = prepareScanNodes();

        // 3. Prepare sink fragment
        List<Long> partitionIds = getAllPartitionIds();

        // For stream/routine load, we only need one fragment, ScanNode -> DataSink.
        // OlapTableSink can dispatch data to corresponding node.
        PlanFragment sinkFragment = new PlanFragment(new PlanFragmentId(0), scanNode, DataPartition.UNPARTITIONED);
        
        // routine load doesn't support pipeline now
        prepareSinkFragment(sinkFragment, partitionIds, false, true);

        fragments.add(sinkFragment);

        // 4. finalize
        for (PlanFragment fragment : fragments) {
            fragment.createDataSink(TResultSinkType.MYSQL_PROTOCAL);
        }
        Collections.reverse(fragments);
    }

    private void generateTupleDescriptor(List<Column> destColumns, boolean isPrimaryKey) throws UserException {
        this.tupleDesc = descTable.createTupleDescriptor("DestTableTupleDescriptor");
        // Add column slotDesc for dest table
        for (Column col : destColumns) {
            SlotDescriptor slotDesc = descTable.addSlotDescriptor(tupleDesc);
            slotDesc.setIsMaterialized(true);
            slotDesc.setType(col.getType());
            slotDesc.setColumn(col);
            slotDesc.setIsNullable(col.isAllowNull());
            if (routimeStreamLoadNegative && !col.isKey() && col.getAggregationType() != AggregateType.SUM) {
                throw new DdlException("Column is not SUM AggreateType. column:" + col.getName());
            }

            if (col.getType().isVarchar() && enableDictOptimize && IDictManager.getInstance().hasGlobalDict(destTable.getId(),
                    col.getName())) {
                Optional<ColumnDict> dict = IDictManager.getInstance().getGlobalDict(destTable.getId(), col.getName());
                dict.ifPresent(columnDict -> globalDicts.add(new Pair<>(slotDesc.getId().asInt(), columnDict)));
            }
        }
        // Add op type slotdesc for primary tabale
        if (isPrimaryKey) {
            SlotDescriptor slotDesc = descTable.addSlotDescriptor(tupleDesc);
            slotDesc.setIsMaterialized(true);
            slotDesc.setColumn(new Column(Load.LOAD_OP_COLUMN, Type.TINYINT));
            slotDesc.setIsNullable(false);
        }
        descTable.computeMemLayout();
    }

    private ScanNode prepareScanNodes() throws UserException {
        ScanNode scanNode = null;
        if (this.etlJobType == EtlJobType.BROKER) {
            FileScanNode fileScanNode = new FileScanNode(new PlanNodeId(planNodeGenerator.getNextId().asInt()), tupleDesc, 
                    "FileScanNode", fileStatusesList, filesAdded);
            fileScanNode.setLoadInfo(loadJobId, txnId, destTable, brokerDesc, fileGroups, strictMode, parallelInstanceNum);
            fileScanNode.setUseVectorizedLoad(true);
            fileScanNode.init(analyzer);
            fileScanNode.finalizeStats(analyzer);
            scanNode = fileScanNode;
        } else if (this.etlJobType == EtlJobType.STREAM_LOAD || this.etlJobType == EtlJobType.ROUTINE_LOAD) {
            StreamLoadScanNode streamScanNode = new StreamLoadScanNode(loadId, new PlanNodeId(0), tupleDesc, 
                    destTable, streamLoadTask);
            streamScanNode.setUseVectorizedLoad(true);
            streamScanNode.init(analyzer);
            streamScanNode.finalizeStats(analyzer);
            scanNode = streamScanNode;
        }
        scanNodes.add(scanNode);
        return scanNode;
    }

    private void prepareSinkFragment(PlanFragment sinkFragment, List<Long> partitionIds, boolean canUsePipeLine, 
            boolean completeTabletSink) throws UserException {
        DataSink dataSink = null;
        if (destTable instanceof OlapTable) {
            dataSink = new OlapTableSink((OlapTable) destTable, tupleDesc, partitionIds, canUsePipeLine);
            if (completeTabletSink) {
                ((OlapTableSink) dataSink).init(loadId, txnId, dbId, timeoutS);
                ((OlapTableSink) dataSink).complete();
            }
            // At present, we only support dop=1 for olap table sink.
            // because tablet writing needs to know the number of senders in advance
            // and guaranteed order of data writing
            // It can be parallel only in some scenes, for easy use 1 dop now.
            sinkFragment.setPipelineDop(1);
            if (this.etlJobType == EtlJobType.BROKER) {
                sinkFragment.setParallelExecNum(parallelInstanceNum);
            }
            //if sink is OlapTableSink Assigned to Be execute this sql [cn execute OlapTableSink will crash]
            context.getSessionVariable().setPreferComputeNode(false);
            context.getSessionVariable().setUseComputeNodes(0);
        } else if (destTable instanceof MysqlTable) {
            dataSink = new MysqlTableSink((MysqlTable) destTable);
        } else {
            throw new SemanticException("Unknown table type " + destTable.getType());
        }
        sinkFragment.setSink(dataSink);
        // After data loading, we need to check the global dict for low cardinality string column
        // whether update.
        sinkFragment.setLoadGlobalDicts(globalDicts);
    }

    public void completeTableSink(long txnId) throws AnalysisException, UserException {
        if (destTable instanceof OlapTable) {
            OlapTableSink dataSink = (OlapTableSink) fragments.get(0).getSink();
            dataSink.init(loadId, txnId, dbId, timeoutS);
            dataSink.complete();
        }
        this.txnId = txnId;
    }


    private List<Long> getAllPartitionIds() throws LoadException {
        Set<Long> partitionIds = Sets.newHashSet();
        OlapTable olapDestTable = (OlapTable) destTable;
        if (this.etlJobType == EtlJobType.BROKER) {
            for (BrokerFileGroup brokerFileGroup : fileGroups) {
                if (brokerFileGroup.getPartitionIds() != null) {
                    partitionIds.addAll(brokerFileGroup.getPartitionIds());
                }
                // all file group in fileGroups should have same partitions, so only need to get partition ids
                // from one of these file groups
                break;
            }
        } else if (this.etlJobType == EtlJobType.STREAM_LOAD || this.etlJobType == etlJobType.ROUTINE_LOAD) {
            PartitionNames partitionNames = streamLoadTask.getPartitions();
            if (partitionNames != null) {
                for (String partName : partitionNames.getPartitionNames()) {
                    Partition part = olapDestTable.getPartition(partName, partitionNames.isTemp());
                    if (part == null) {
                        throw new LoadException("unknown partition " + partName + " in table " + destTable.getName());
                    }
                    partitionIds.add(part.getId());
                }
            }
        }

        if (partitionIds.isEmpty()) {
            for (Partition partition : destTable.getPartitions()) {
                partitionIds.add(partition.getId());
            }
        }

        // If this is a dynamic partitioned table, it will take some time to create the partition after the
        // table is created, a exception needs to be thrown here
        if (partitionIds.isEmpty()) {
            throw new LoadException("data cannot be inserted into table with empty partition. " +
                    "Use `SHOW PARTITIONS FROM " + destTable.getName() +
                    "` to see the currently partitions of this table. ");
        }

        return Lists.newArrayList(partitionIds);
    }

    public void updateLoadInfo(TUniqueId loadId) {
        for (PlanFragment planFragment : fragments) {
            if (!(planFragment.getSink() instanceof OlapTableSink
                    && planFragment.getPlanRoot() instanceof FileScanNode)) {
                continue;
            }

            // when retry load by reusing this plan in load process, the load_id should be changed
            OlapTableSink olapTableSink = (OlapTableSink) planFragment.getSink();
            olapTableSink.updateLoadId(loadId);
            LOG.info("update olap table sink's load id to {}, job: {}", DebugUtil.printId(loadId), loadJobId);

            // update backend and broker
            FileScanNode fileScanNode = (FileScanNode) planFragment.getPlanRoot();
            fileScanNode.updateScanRangeLocations();
        }
    }

    public Boolean needShufflePlan() {
        OlapTable olapDestTable = (OlapTable) destTable;
        if (KeysType.DUP_KEYS.equals(olapDestTable.getKeysType())) {
            return false;
        }

        if (olapDestTable.getDefaultReplicationNum() <= 1) {
            return false;
        }

        if (KeysType.AGG_KEYS.equals(olapDestTable.getKeysType())) {
            for (Map.Entry<Long, List<Column>> entry : olapDestTable.getIndexIdToSchema().entrySet()) {
                List<Column> schema = entry.getValue();
                for (Column column : schema) {
                    if (column.getAggregationType() == AggregateType.REPLACE
                            || column.getAggregationType() == AggregateType.REPLACE_IF_NOT_NULL) {
                        return true;
                    }
                }
            }
            return false;
        }

        return true;
    }

    private void castLiteralToTargetColumnsType(InsertStmt insertStatement) {
        Preconditions.checkState(insertStatement.getQueryStatement().getQueryRelation() instanceof ValuesRelation,
                "must values");
        List<Column> fullSchema = insertStatement.getTargetTable().getFullSchema();
        ValuesRelation values = (ValuesRelation) insertStatement.getQueryStatement().getQueryRelation();
        RelationFields fields = insertStatement.getQueryStatement().getQueryRelation().getRelationFields();
        for (int columnIdx = 0; columnIdx < insertStatement.getTargetTable().getBaseSchema().size(); ++columnIdx) {
            Column targetColumn = fullSchema.get(columnIdx);
            if (insertStatement.getTargetColumnNames() == null) {
                for (List<Expr> row : values.getRows()) {
                    if (row.get(columnIdx) instanceof DefaultValueExpr) {
                        row.set(columnIdx, new StringLiteral(targetColumn.calculatedDefaultValue()));
                    }
                    row.set(columnIdx, TypeManager.addCastExpr(row.get(columnIdx), targetColumn.getType()));
                }
                fields.getFieldByIndex(columnIdx).setType(targetColumn.getType());
            } else {
                int idx = insertStatement.getTargetColumnNames().indexOf(targetColumn.getName().toLowerCase());
                if (idx != -1) {
                    for (List<Expr> row : values.getRows()) {
                        if (row.get(idx) instanceof DefaultValueExpr) {
                            row.set(idx, new StringLiteral(targetColumn.calculatedDefaultValue()));
                        }
                        row.set(idx, TypeManager.addCastExpr(row.get(idx), targetColumn.getType()));
                    }
                    fields.getFieldByIndex(idx).setType(targetColumn.getType());
                }
            }
        }
    }

    private OptExprBuilder fillDefaultValue(LogicalPlan logicalPlan, ColumnRefFactory columnRefFactory,
                                            InsertStmt insertStatement, List<ColumnRefOperator> outputColumns) {
        List<Column> baseSchema = insertStatement.getTargetTable().getBaseSchema();
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();

        for (int columnIdx = 0; columnIdx < baseSchema.size(); ++columnIdx) {
            Column targetColumn = baseSchema.get(columnIdx);
            if (insertStatement.getTargetColumnNames() == null) {
                outputColumns.add(logicalPlan.getOutputColumn().get(columnIdx));
                columnRefMap.put(logicalPlan.getOutputColumn().get(columnIdx),
                        logicalPlan.getOutputColumn().get(columnIdx));
            } else {
                int idx = insertStatement.getTargetColumnNames().indexOf(targetColumn.getName().toLowerCase());
                if (idx == -1) {
                    ScalarOperator scalarOperator;
                    Column.DefaultValueType defaultValueType = targetColumn.getDefaultValueType();
                    if (defaultValueType == Column.DefaultValueType.NULL) {
                        scalarOperator = ConstantOperator.createNull(targetColumn.getType());
                    } else if (defaultValueType == Column.DefaultValueType.CONST) {
                        scalarOperator = ConstantOperator.createVarchar(targetColumn.calculatedDefaultValue());
                    } else if (defaultValueType == Column.DefaultValueType.VARY) {
                        if (SUPPORTED_DEFAULT_FNS.contains(targetColumn.getDefaultExpr().getExpr())) {
                            scalarOperator = SqlToScalarOperatorTranslator.
                                    translate(targetColumn.getDefaultExpr().obtainExpr());
                        } else {
                            throw new SemanticException(
                                    "Column:" + targetColumn.getName() + " has unsupported default value:"
                                            + targetColumn.getDefaultExpr().getExpr());
                        }
                    } else {
                        throw new SemanticException("Unknown default value type:%s", defaultValueType.toString());
                    }
                    ColumnRefOperator col = columnRefFactory
                            .create(scalarOperator, scalarOperator.getType(), scalarOperator.isNullable());

                    outputColumns.add(col);
                    columnRefMap.put(col, scalarOperator);
                } else {
                    outputColumns.add(logicalPlan.getOutputColumn().get(idx));
                    columnRefMap.put(logicalPlan.getOutputColumn().get(idx), logicalPlan.getOutputColumn().get(idx));
                }
            }
        }
        return logicalPlan.getRootBuilder().withNewRoot(new LogicalProjectOperator(new HashMap<>(columnRefMap)));
    }

    private OptExprBuilder fillShadowColumns(ColumnRefFactory columnRefFactory, InsertStmt insertStatement,
                                             List<ColumnRefOperator> outputColumns, OptExprBuilder root,
                                             ConnectContext session) {
        List<Column> fullSchema = insertStatement.getTargetTable().getFullSchema();
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();

        for (int columnIdx = 0; columnIdx < fullSchema.size(); ++columnIdx) {
            Column targetColumn = fullSchema.get(columnIdx);

            if (targetColumn.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX) ||
                    targetColumn.isNameWithPrefix(SchemaChangeHandler.SHADOW_NAME_PRFIX_V1)) {
                String originName = Column.removeNamePrefix(targetColumn.getName());
                Column originColumn = fullSchema.stream()
                        .filter(c -> c.nameEquals(originName, false)).findFirst().get();
                ColumnRefOperator originColRefOp = outputColumns.get(fullSchema.indexOf(originColumn));

                ColumnRefOperator columnRefOperator = columnRefFactory.create(
                        targetColumn.getName(), targetColumn.getType(), targetColumn.isAllowNull());

                outputColumns.add(columnRefOperator);
                columnRefMap.put(columnRefOperator, new CastOperator(targetColumn.getType(), originColRefOp, true));
                continue;
            }

            if (targetColumn.isNameWithPrefix(CreateMaterializedViewStmt.MATERIALIZED_VIEW_NAME_PREFIX)) {
                String originName = targetColumn.getRefColumn().getColumnName();
                Column originColumn = fullSchema.stream()
                        .filter(c -> c.nameEquals(originName, false)).findFirst().get();
                ColumnRefOperator originColRefOp = outputColumns.get(fullSchema.indexOf(originColumn));

                ExpressionAnalyzer.analyzeExpression(targetColumn.getDefineExpr(), new AnalyzeState(),
                        new Scope(RelationId.anonymous(),
                                new RelationFields(insertStatement.getTargetTable().getBaseSchema().stream()
                                        .map(col -> new Field(col.getName(), col.getType(),
                                                new TableName(null, insertStatement.getTargetTable().getName()), null))
                                        .collect(Collectors.toList()))), session);

                ExpressionMapping expressionMapping =
                        new ExpressionMapping(new Scope(RelationId.anonymous(), new RelationFields()),
                                Lists.newArrayList());
                expressionMapping.put(targetColumn.getRefColumn(), originColRefOp);
                ScalarOperator scalarOperator =
                        SqlToScalarOperatorTranslator.translate(targetColumn.getDefineExpr(), expressionMapping,
                                columnRefFactory);

                ColumnRefOperator columnRefOperator =
                        columnRefFactory.create(scalarOperator, scalarOperator.getType(), scalarOperator.isNullable());
                outputColumns.add(columnRefOperator);
                columnRefMap.put(columnRefOperator, scalarOperator);
                continue;
            }

            // columnIdx >= outputColumns.size() mean this is a new add schema change column
            if (columnIdx >= outputColumns.size()) {
                ColumnRefOperator columnRefOperator = columnRefFactory.create(
                        targetColumn.getName(), targetColumn.getType(), targetColumn.isAllowNull());
                outputColumns.add(columnRefOperator);

                Column.DefaultValueType defaultValueType = targetColumn.getDefaultValueType();
                if (defaultValueType == Column.DefaultValueType.NULL) {
                    columnRefMap.put(columnRefOperator, ConstantOperator.createNull(targetColumn.getType()));
                } else if (defaultValueType == Column.DefaultValueType.CONST) {
                    columnRefMap.put(columnRefOperator, ConstantOperator.createVarchar(
                            targetColumn.calculatedDefaultValue()));
                } else if (defaultValueType == Column.DefaultValueType.VARY) {
                    throw new SemanticException("Column:" + targetColumn.getName() + " has unsupported default value:"
                            + targetColumn.getDefaultExpr().getExpr());
                }
            } else {
                columnRefMap.put(outputColumns.get(columnIdx), outputColumns.get(columnIdx));
            }
        }
        return root.withNewRoot(new LogicalProjectOperator(new HashMap<>(columnRefMap)));
    }

    private OptExprBuilder castOutputColumnsTypeToTargetColumns(ColumnRefFactory columnRefFactory,
                                                                InsertStmt insertStatement,
                                                                List<ColumnRefOperator> outputColumns,
                                                                OptExprBuilder root) {
        List<Column> fullSchema = insertStatement.getTargetTable().getFullSchema();
        Map<ColumnRefOperator, ScalarOperator> columnRefMap = new HashMap<>();
        ScalarOperatorRewriter rewriter = new ScalarOperatorRewriter();
        List<ScalarOperatorRewriteRule> rewriteRules = Arrays.asList(new FoldConstantsRule());
        for (int columnIdx = 0; columnIdx < fullSchema.size(); ++columnIdx) {
            if (!fullSchema.get(columnIdx).getType().matchesType(outputColumns.get(columnIdx).getType())) {
                Column c = fullSchema.get(columnIdx);
                ColumnRefOperator k = columnRefFactory.create(c.getName(), c.getType(), c.isAllowNull());
                ScalarOperator castOperator = new CastOperator(fullSchema.get(columnIdx).getType(),
                        outputColumns.get(columnIdx), true);
                columnRefMap.put(k, rewriter.rewrite(castOperator, rewriteRules));
                outputColumns.set(columnIdx, k);
            } else {
                columnRefMap.put(outputColumns.get(columnIdx), outputColumns.get(columnIdx));
            }
        }
        return root.withNewRoot(new LogicalProjectOperator(new HashMap<>(columnRefMap)));
    }

    /**
     * OlapTableSink may be executed in multiply fragment instances of different machines
     * For non-duplicate key types, we must guarantee that the orders of the same key are
     * exactly the same. In order to achieve this goal, we can perform shuffle before TableSink
     * so that the same key will be sent to the same fragment instance
     */
    private PhysicalPropertySet createPhysicalPropertySet(InsertStmt insertStmt,
                                                          List<ColumnRefOperator> outputColumns) {
        QueryRelation queryRelation = insertStmt.getQueryStatement().getQueryRelation();
        if ((queryRelation instanceof SelectRelation && queryRelation.hasLimit())) {
            DistributionProperty distributionProperty =
                    new DistributionProperty(new GatherDistributionSpec(queryRelation.getLimit().getLimit()));
            return new PhysicalPropertySet(distributionProperty);
        }

        if (!(insertStmt.getTargetTable() instanceof OlapTable)) {
            return new PhysicalPropertySet();
        }

        OlapTable table = (OlapTable) insertStmt.getTargetTable();

        if (KeysType.DUP_KEYS.equals(table.getKeysType())) {
            return new PhysicalPropertySet();
        }

        // No extra distribution property is needed if replication num is 1
        if (!enableSingleReplicationShuffle && table.getDefaultReplicationNum() <= 1) {
            return new PhysicalPropertySet();
        }

        List<Column> columns = table.getFullSchema();
        Preconditions.checkState(columns.size() == outputColumns.size(),
                "outputColumn's size must equal with table's column size");

        List<Column> keyColumns = table.getKeyColumnsByIndexId(table.getBaseIndexId());
        List<Integer> keyColumnIds = Lists.newArrayList();
        keyColumns.forEach(column -> {
            int index = columns.indexOf(column);
            Preconditions.checkState(index >= 0);
            keyColumnIds.add(outputColumns.get(index).getId());
        });

        HashDistributionDesc desc =
                new HashDistributionDesc(keyColumnIds, HashDistributionDesc.SourceType.SHUFFLE_AGG);
        DistributionSpec spec = DistributionSpec.createHashDistributionSpec(desc);
        DistributionProperty property = new DistributionProperty(spec);
        return new PhysicalPropertySet(property);
    }

    public ConnectContext getContext() {
        return context;
    }

    public DescriptorTable getDescTable() {
        return descTable;
    }

    public long getLoadJobId() {
        return loadJobId;
    }

    public void setLoadJobId(long loadJobId) {
        this.loadJobId = loadJobId;
    }

    public TUniqueId getLoadId() {
        return loadId;
    }

    public List<PlanFragment> getFragments() {
        return fragments;
    }

    public List<ScanNode> getScanNodes() {
        return scanNodes;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public String getTimeZone() {
        return timezone;
    }

    public long getTimeout() {
        return timeoutS;
    }

    public void setExecMemLimit(long execMemLimit) {
        this.execMemLimit = execMemLimit;
    }

    public long getExecMemLimit() {
        return execMemLimit;
    }

    public void setLoadMemLimit(long loadMemLimit) {
        this.loadMemLimit = loadMemLimit;
    }

    public long getLoadMemLimit() {
        return loadMemLimit;
    }

    public EtlJobType getEtlJobType() {
        return etlJobType;
    }

    public Map<String, String> getSessionVariables() {
        return sessionVariables;
    }
}
