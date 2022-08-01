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


def raw_counter(title, metric, unit, *group):
    global datasource

    group = [*group]
    if len(group) > 0:
        target = Target(
            datasource=datasource,
            expr='sum(rate({}[1m])) by ({})'.format(metric, ','.join(group)),
            legendFormat="{{ handler }}",
            refId='A',
        )
    else:
        target = Target(
            datasource=datasource,
            expr='rate({}[1m])'.format(metric),
            legendFormat="{{ handler }}",
            refId='A',
        )
    return TimeSeries(
        title=title,
        dataSource=datasource,
        targets=[target],
        unit=unit,
        gridPos=next_pos(),
    )


def simple_total(title, metric, unit=NO_FORMAT):
    return raw_counter(title, metric, unit)


def raw_histogram(title, metric, unit, *group):
    global datasource
    le_group = ','.join(['le', *group])
    group = [*group]
    if len(group) > 0:
        group = ','.join(group)
        avg_target = Target(
            datasource=datasource,
            expr=
            'sum(rate({}_sum[1m])) by ({}) / sum(rate({}_count[1m])) by ({})'.
            format(metric, group, metric, group),
            legendFormat="avg",
            refId='C',
        )
    else:
        avg_target = Target(
            datasource=datasource,
            expr='sum(rate({}_sum[1m])) / sum(rate({}_count[1m]))'.format(
                metric, metric),
            legendFormat="avg",
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
                legendFormat="99%",
                refId='A',
            ),
            Target(
                datasource=datasource,
                expr='histogram_quantile(0.95, sum(rate({}_bucket[1m])) by ({}))'
                .format(metric, le_group),
                legendFormat="95%",
                refId='B',
            ),
            avg_target,
        ],
        unit=unit,
        gridPos=next_pos(),
    )


def simple_duration_seconds(title, metric):
    return raw_histogram(title, metric, SECONDS_FORMAT)


def simple_histogram_size(title, metric):
    return raw_histogram(title, metric, NO_FORMAT)


def vector_total(title, metric, group):
    return raw_counter(title, metric, NO_FORMAT, group)


def vector_duration_seconds(title, metric, *group):
    return raw_histogram(title, metric, SECONDS_FORMAT, *group)


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
    return row_panels("Root services", *panels)


dashboard = Dashboard(
    title="Engula Server",
    description=
    "Engula Server dashboard using the default Prometheus datasource",
    tags=['engula', 'server'],
    timezone="browser",
    panels=[
        node_service_batch_panels(),
        node_service_shard_migration_panels(),
        node_service_metadata_panels(),
        node_panels(),
        node_job_panels(),
        raft_service_panels(),
        raft_group_panels(),
        raft_group_snapshot_panels(),
        root_service_panels(),
    ],
).auto_panel_ids()
