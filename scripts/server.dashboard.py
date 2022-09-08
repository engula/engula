#!/usr/bin/python3

from grafanalib.core import (
    Dashboard,
    TimeSeries,
    GaugePanel,
    Target,
    GridPos,
    RowPanel,
    NO_FORMAT,
    BYTES_FORMAT,
    SECONDS_FORMAT,
)
from grafanalib._gen import DashboardEncoder
import json
import requests
from os import getenv

x = 0
y = 0
datasource = {"type": "promethues"}


def next_pos():
    global x, y

    pos = GridPos(h=9, w=12, x=x, y=y)
    x += 12
    if x >= 24:
        x = 0
        y += 9
    return pos


def row_panels(title, *panels):
    global x, y
    x = 0
    y += 9

    return RowPanel(
        title=title,
        collapsed=True,
        gridPos=GridPos(h=1, w=24, x=0, y=y),
        panels=[*panels],
    )


def raw_counter(title, metric, unit, legend, *group):
    global datasource

    group = [*group]
    if len(group) > 0:
        target = Target(
            datasource=datasource,
            expr='sum(rate({}[1m])) by ({})'.format(metric, ','.join(group)),
            legendFormat="{{{{ {} }}}}".format(legend),
            refId='A',
        )
    else:
        target = Target(
            datasource=datasource,
            expr='rate({}[1m])'.format(metric),
            legendFormat="{{{{ {} }}}}".format(legend),
            refId='A',
        )
    return TimeSeries(
        title=title,
        dataSource=datasource,
        targets=[target],
        unit=unit,
        gridPos=next_pos(),
    )


def raw_gauge_ts(title, metric, legend="instance", unit=NO_FORMAT):
    global datasource
    if legend == "instance":
        expr = '{}'.format(metric)
    else:
        expr = 'sum({}) by ({})'.format(metric, legend)
    target = Target(
        datasource=datasource,
        expr=expr,
        legendFormat='{{{{ {} }}}}'.format(legend),
        refId='A',
    )
    return TimeSeries(
        title=title,
        dataSource=datasource,
        targets=[target],
        gridPos=next_pos(),
        unit=unit,
    )


def raw_gauge(title, metric):
    global datasource
    target = Target(
        datasource=datasource,
        expr='{}'.format(metric),
        refId='A',
    )
    return GaugePanel(
        title=title,
        dataSource=datasource,
        targets=[target],
        gridPos=next_pos(),
    )


def simple_total(title, metric, legend="handler", unit=NO_FORMAT):
    return raw_counter(title, metric, unit, legend)


def raw_histogram(title, metric, unit, *group):
    global datasource
    legend_prefix = ""
    le_group = ','.join(['le', *group])
    group = [*group]
    if len(group) > 0:
        legend_prefix = '{{{{ {} }}}}-'.format(*group)
        group = ','.join(group)
        avg_target = Target(
            datasource=datasource,
            expr=
            'sum(rate({}_sum[1m])) by ({}) / sum(rate({}_count[1m])) by ({})'.
            format(metric, group, metric, group),
            legendFormat="{}avg".format(legend_prefix),
            refId='C',
        )
    else:
        avg_target = Target(
            datasource=datasource,
            expr='sum(rate({}_sum[1m])) / sum(rate({}_count[1m]))'.format(
                metric, metric),
            legendFormat="{}avg".format(legend_prefix),
            refId='C',
        )
    return TimeSeries(
        title=title,
        dataSource=datasource,
        targets=[
            Target(
                datasource=datasource,
                expr='histogram_quantile(0.99, sum(rate({}_bucket[1m])) by ({}))'
                .format(metric, le_group),
                legendFormat="{}99%".format(legend_prefix),
                refId='A',
            ),
            Target(
                datasource=datasource,
                expr='histogram_quantile(0.95, sum(rate({}_bucket[1m])) by ({}))'
                .format(metric, le_group),
                legendFormat="{}95%".format(legend_prefix),
                refId='B',
            ),
            avg_target,
        ],
        unit=unit,
        gridPos=next_pos(),
    )


def simple_duration_seconds(title, metric):
    return raw_histogram(title, metric, SECONDS_FORMAT)


def simple_histogram_size(title, metric, unit=NO_FORMAT):
    return raw_histogram(title, metric, unit)


def vector_total(title, metric, group, legend="handler", unit=NO_FORMAT):
    return raw_counter(title, metric, unit, legend, group)


def vector_duration_seconds(title, metric, *group):
    return raw_histogram(title, metric, SECONDS_FORMAT, *group)


def proxy_service_db_panels():
    return row_panels(
        "Proxy services - DB",
        vector_total("request qps", "proxy_service_database_request_total",
                     "type"),
        vector_duration_seconds(
            "request duration",
            "proxy_service_database_request_duration_seconds", "type"),
    )


def node_service_batch_panels():
    return row_panels(
        "Node services - Batch",
        simple_total("batch qps", "node_service_batch_request_total"),
        simple_duration_seconds("batch duration",
                                "node_service_batch_request_duration_seconds"),
        vector_total("group request qps", "node_service_group_request_total",
                     "type"),
        vector_duration_seconds("group request duration",
                                "node_service_group_request_duration_seconds",
                                "type"),
        simple_histogram_size("requests per batch",
                              "node_service_batch_request_size"),
    )


def node_service_shard_migration_panels():
    return row_panels(
        "Node services - Shard migration",
        simple_total("migrate qps", "node_service_migrate_request_total"),
        simple_duration_seconds(
            "migration duration",
            "node_service_migrate_request_duration_seconds"),
        simple_total("pull qps", "node_service_pull_request_total"),
        simple_duration_seconds("pull duration",
                                "node_service_pull_request_duration_seconds"),
        simple_total("forward qps", "node_service_forward_request_total"),
        simple_duration_seconds(
            "forward duration",
            "node_service_forward_request_duration_seconds"),
    )


def node_service_metadata_panels():
    return row_panels(
        "Node services - Metadata",
        simple_total("get_root qps", "node_service_get_root_request_total"),
        simple_duration_seconds(
            "get_root duration",
            "node_service_get_root_request_duration_seconds"),
        simple_total("create_replica qps",
                     "node_service_create_replica_request_total"),
        simple_duration_seconds(
            "create_replica duration",
            "node_service_create_replica_request_duration_seconds"),
        simple_total("remove_replica qps",
                     "node_service_remove_replica_request_total"),
        simple_duration_seconds(
            "remove_replica duration",
            "node_service_remove_replica_request_duration_seconds"),
        simple_total("root_heartbeat qps",
                     "node_service_root_heartbeat_request_total"),
        simple_duration_seconds(
            "root_heartbeat duration",
            "node_service_root_heartbeat_request_duration_seconds"),
    )


def raft_service_panels():
    return row_panels(
        "Raft services",
        simple_total("raft msg qps", "raft_service_msg_request_total"),
        simple_total("raft snapshot qps",
                     "raft_service_snapshot_request_total"),
        simple_histogram_size("batch per msg", "raft_service_msg_batch_size"),
    )


def raft_group_panels():
    return row_panels(
        "Raft Group",
        vector_total("raft propose qps", "raftgroup_propose_total", "type"),
        vector_duration_seconds("raft propose duration",
                                "raftgroup_propose_duration_seconds", "type"),
        vector_total("raft read qps", "raftgroup_read_total", "type"),
        vector_duration_seconds("raft read duration",
                                "raftgroup_read_duration_seconds", "type"),
        simple_total("raft config change qps",
                     "raftgroup_config_change_total"),
        simple_total("raft transfer leader qps",
                     "raftgroup_transfer_leader_total"),
        simple_total("raft unreachable qps", "raftgroup_unreachable_total"),
        simple_total("raft worker advance total",
                     "raftgroup_worker_advance_total"),
    )


def raft_group_snapshot_panels():
    return row_panels(
        "Raft Group - Snapshot",
        simple_total("create qps", "raftgroup_create_snapshot_total"),
        simple_duration_seconds("create duration",
                                "raftgroup_create_snapshot_duration_seconds"),
        simple_total("apply qps", "raftgroup_apply_snapshot_total"),
        simple_duration_seconds("apply duration",
                                "raftgroup_apply_snapshot_duration_seconds"),
        simple_total("send qps", "raftgroup_send_snapshot_total"),
        simple_total("send bytes", "raftgroup_send_snapshot_bytes_total",
                     BYTES_FORMAT),
        simple_total("download qps", "raftgroup_download_snapshot_total"),
        simple_total("download bytes",
                     "raftgroup_download_snapshot_bytes_total", BYTES_FORMAT),
        simple_duration_seconds(
            "download duration",
            "raftgroup_download_snapshot_duration_seconds"),
    )


def raft_engine_panels():
    return row_panels(
        "Raft Engine",
        raw_gauge_ts("memory usage", "raft_engine_log_entry_count", unit=BYTES_FORMAT),
        raw_gauge_ts("memory usage",
                     "raft_engine_log_entry_count",
                     unit=BYTES_FORMAT),
        simple_histogram_size("write size",
                              "raft_engine_write_size",
                              unit=BYTES_FORMAT),
        simple_duration_seconds("write duration",
                                "raft_engine_write_duration_seconds"),
        simple_duration_seconds("apply log entries duration",
                                "raft_engine_write_apply_duration_seconds"),
        simple_duration_seconds("sync log file duration",
                                "raft_engine_sync_log_duration_seconds"),
        simple_duration_seconds("rotate log file duration",
                                "raft_engine_rotate_log_duration_seconds"),
        simple_duration_seconds("allocate log file duration",
                                "raft_engine_allocate_log_duration_seconds"),
    )


def node_panels():
    return row_panels(
        "Node",
        simple_total("replica retry qps", "node_retry_total"),
    )


def node_job_panels():
    return row_panels(
        "Node - Jobs",
        simple_total("pull shard qps", "node_pull_shard_total"),
        simple_duration_seconds("pull shard duration",
                                "node_pull_shard_duration_seconds"),
        simple_total("report qps", "node_report_total"),
        simple_duration_seconds("report duration",
                                "node_report_duration_seconds"),
        simple_total("destory replica qps", "node_destory_replica_total"),
        simple_duration_seconds("destory replica duration",
                                "node_destory_replica_duration_seconds"),
    )


def root_service_panels():
    methods = ['report', 'watch', 'admin', 'join', 'alloc_replica']
    panels = []
    for method in methods:
        panels.append(
            simple_total('{} qps'.format(method),
                         'root_service_{}_request_total'.format(method)))
        panels.append(
            simple_duration_seconds(
                '{} duration'.format(method),
                'root_service_{}_request_duration_seconds'.format(method)))
    return row_panels("Root - Services", *panels)


def root_cluster_overview_panels():
    return row_panels(
        "Root - Cluster Overview",
        raw_gauge_ts("node count", "cluster_node_total", "type"),
        raw_gauge_ts("group count", "cluster_group_total", "job"),
        raw_gauge_ts("root reconcile cluster count-based balanced status",
                     "root_reconcile_already_balanced_info", "type"),
        raw_gauge_ts("node replica count",
                     "cluster_node_replica_total", "node"),
        raw_gauge_ts("node leader count", "cluster_node_leader_total", "node"),
        raw_gauge_ts("group shard count",
                     "cluster_group_shard_total", "group"),
    )


def root_misc_panels():
    return row_panels(
        "Root - Misc",
        raw_gauge_ts("node as leader", "root_service_node_as_leader_info"),
        simple_total("root bootstrap fail count",
                     "root_boostrap_fail_total", "instance"),
        simple_duration_seconds(
            "root bootstrap duration", "root_bootstrap_duration_seconds"),
        raw_gauge_ts("root watch table size", "root_watch_table_size", "job"),
        simple_duration_seconds(
            "root notify watcher duration", "root_watch_notify_duration_seconds"),
    )


def root_reconcile_panels():
    return row_panels(
        "Root - Reconcile",
        simple_duration_seconds("root reconcile check and prepare task duration",
                                "root_reconcile_scheduler_check_task_duration_seconds"),
        simple_duration_seconds(
            "root reconcile per tick execute duration", "root_reconcile_step_duration_seconds"),
        raw_gauge_ts("root reconcile scheduler task queue size",
                     "root_reconcile_scheduler_task_queue_size", "job"),
        vector_total(
            "root reconcile handle task count", "root_reconcile_scheduler_task_handle_total", "type", "type"),
        vector_duration_seconds("root reconcile handle per task duration",
                                "root_reconcile_scheduler_task_handle_duration_seconds", "type"),
        vector_total("root reconcile retry count per task-type",
                     "root_reconcile_scheduler_task_retry_total", "type", "type"),
        vector_duration_seconds("root reconcile create new group per step duration",
                                "root_reconcile_scheduler_create_group_step_duration_seconds", "type"),
        vector_duration_seconds("root reconcile reallocate replica per step duration",
                                "root_reconcile_scheduler_reallocate_replica_step_duration_seconds", "type"),
        vector_duration_seconds("root reconcile create collection shards per step duration",
                                "root_reconcile_scheduler_create_collection_step_duration_seconds", "type"),
    )


def root_hearbeat_report_panels():
    return row_panels(
        "Root - Hearbeat Report",
        simple_duration_seconds("root heartbeat per tick duration",
                                "root_heartbeat_step_duration_seconds"),
        vector_total("root heartbeat fail count",
                     "root_heartbeat_fail_total", "node", "node"),
        simple_duration_seconds("root heartbeat rpc duration",
                                "root_heartbeat_rpc_nodes_duration_seconds"),
        simple_duration_seconds("root heartbeat handle group_detail duration",
                                "root_heartbeat_handle_group_detail_seconds"),
        simple_duration_seconds(
            "root heartbeat handle node_stats duration", "root_heartbeat_handle_node_stats_seconds"),
        simple_total("root heartbeat actually update node stats count",
                     "root_heartbeat_update_node_stats_total"),
        vector_total("root heartbeat or report actually update group desc count",
                     "root_update_group_desc_total", "type", "type"),
        vector_total("root heartbeat or report actually update replica state count",
                     "root_update_replica_state_total", "type", "type"),
    )


def client_database_panels():
    return row_panels(
        "Client - Database",
        vector_total("database request qps",
            "client_database_request_total", "type", "type"),
        vector_duration_seconds(
            "database request duration",
            "client_database_request_duration_seconds", "type"),
        vector_total("database in/out bytes",
            "client_database_bytes_total", "type", "type", BYTES_FORMAT),
    )


def client_group_panels():
    return row_panels(
        "Client - Group",
        vector_total("group request qps",
            "group_client_group_request_total", "type", "type"),
        vector_duration_seconds("group request duration",
            "group_client_group_request_duration_seconds", "type", "type"),
        simple_total("group client retry total",
            "group_client_retry_total"),
    )


def executor_panels():
    return row_panels(
        "Executor",
        simple_total("Total park", "executor_park_total"),
        vector_total("Total spawn", "executor_spawn_total", "priority", "priority"),
    )


dashboard = Dashboard(
    title="Engula Server",
    description="Engula Server dashboard using the default Prometheus datasource",
    tags=['engula', 'server'],
    timezone="browser",
    panels=[
        proxy_service_db_panels(),
        node_service_batch_panels(),
        node_service_shard_migration_panels(),
        node_service_metadata_panels(),
        node_panels(),
        node_job_panels(),
        raft_service_panels(),
        raft_group_panels(),
        raft_group_snapshot_panels(),
        raft_engine_panels(),
        root_cluster_overview_panels(),
        root_service_panels(),
        root_reconcile_panels(),
        root_hearbeat_report_panels(),
        root_misc_panels(),
        client_database_panels(),
        client_group_panels(),
        executor_panels(),
    ],
).auto_panel_ids()
