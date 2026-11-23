"""拨测任务走节点管理 V3 部署策略的测试。

覆盖范围:
- 路由：已有绑定 / 已有 V2 订阅 / 新任务按开关
- 下发：上下文与 V2 订阅步骤一致，一个任务一条策略，策略 ID 与执行记录落库
- 启停删除：停止、删除切换为反相，启动恢复正向并重新下发
- 状态刷新：按最近一次执行汇总，失败记录节点日志且只记录一次，反相后不改状态

部署策略能力用 Mock 替换，不访问节点管理。
"""

from collections.abc import Iterator
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest

from bk_monitor_base.domains.uptime_check.define import UptimeCheckTaskStatus
from bk_monitor_base.domains.uptime_check.models import (
    UptimeCheckNodeModel,
    UptimeCheckTaskCollectorLog,
    UptimeCheckTaskDeployPolicy,
    UptimeCheckTaskModel,
    UptimeCheckTaskSubscription,
)
from bk_monitor_base.domains.uptime_check.operation import refresh_task_status
from bk_monitor_base.domains.uptime_check.services.deploy_policy import use_deploy_policy
from bk_monitor_base.domains.uptime_check.services.subscription import SubscriptionService
from bk_monitor_base.domains.uptime_check.services.task_manager import DeployTaskError, TaskManager
from bk_monitor_base.infras.constant import DEFAULT_TENANT_ID
from bk_monitor_base.infras.nodeman_control import (
    DeployState,
    ExecutionState,
    HostDeployState,
    SubConfigDeclaration,
    SubConfigPolicies,
)

MODULE = "bk_monitor_base.domains.uptime_check.services.deploy_policy"
DATA_ID = 1011


@pytest.fixture
def policies() -> Iterator[Mock]:
    """替换部署策略能力，默认最近一次执行已成功。"""
    fake = Mock(spec=SubConfigPolicies)
    fake.save.return_value = 88
    fake.execute.side_effect = [f"wf-{index}" for index in range(1, 10)]
    fake.execution_state.return_value = ExecutionState(DeployState.SUCCESS)
    fake.failure_logs.return_value = []
    with patch(f"{MODULE}.sub_config_policies", fake):
        yield fake


@pytest.fixture
def v3_enabled() -> Iterator[None]:
    """开启节点管理 V3。"""
    with patch(f"{MODULE}.nodeman_v3_enabled", return_value=True):
        yield


@pytest.fixture(autouse=True)
def common_mocks() -> Iterator[None]:
    """数据接入、配置读取与租户校验与本测试无关，统一 mock。"""
    config = MagicMock()
    config.domains.uptime_check.base64_encode_trigger_chars = []
    with (
        patch("bk_monitor_base.domains.uptime_check.services.config_generator.get_config", return_value=config),
        patch(
            "bk_monitor_base.domains.uptime_check.services.data_access.DataAccessService.get_or_create_data_id",
            return_value=(False, DATA_ID),
        ),
        patch("bk_monitor_base.domains.uptime_check.operation.bk_biz_id_to_bk_tenant_id", return_value="default"),
    ):
        yield


@pytest.fixture
def task(db: Any) -> UptimeCheckTaskModel:
    """一个 HTTP 拨测任务，含本业务节点和公共业务节点。"""
    task = UptimeCheckTaskModel.objects.create(
        bk_biz_id=2,
        name="V3 拨测任务",
        protocol=UptimeCheckTaskModel.Protocol.HTTP,
        status=UptimeCheckTaskStatus.NEW_DRAFT.value,
        labels={"env": "prod"},
        config={"url_list": ["https://www.example.com"], "method": "GET", "timeout": 3000, "period": 60},
    )
    nodes = [
        UptimeCheckNodeModel.objects.create(
            bk_tenant_id=DEFAULT_TENANT_ID, bk_biz_id=2, name="本业务节点", ip="127.0.0.1", bk_host_id=101, plat_id=0
        ),
        UptimeCheckNodeModel.objects.create(
            bk_tenant_id=DEFAULT_TENANT_ID,
            bk_biz_id=3,
            name="公共节点",
            ip="127.0.0.2",
            bk_host_id=202,
            plat_id=0,
            is_common=True,
        ),
    ]
    task.nodes.add(*nodes)  # pyright: ignore[reportAttributeAccessIssue]
    return task


def _reload(task: UptimeCheckTaskModel) -> UptimeCheckTaskModel:
    return UptimeCheckTaskModel.objects.get(pk=task.pk)


@pytest.mark.parametrize(
    "binding_policy_id,has_subscription,switch,expected",
    [
        (88, False, False, True),  # 已创建部署策略：始终走部署策略，关闭开关也不回退
        (88, True, False, True),
        (None, False, False, False),  # 只有空绑定（首次下发在创建策略前失败）：按开关
        (None, False, True, True),
        ("", True, True, False),  # 已有 V2 订阅的存量任务：继续走订阅，不在这里迁移
        ("", False, True, True),  # 新任务：按开关
        ("", False, False, False),
    ],
)
def test_use_deploy_policy_routing(task, binding_policy_id, has_subscription, switch, expected):
    """路由规则：已创建的部署策略优先，其次存量订阅，最后看 V3 开关。

    binding_policy_id 为空字符串表示没有绑定记录，None 表示有绑定但尚未拿到策略 ID。
    """
    if binding_policy_id != "":
        UptimeCheckTaskDeployPolicy.objects.create(
            uptimecheck_id=task.pk, policy_name="p", deploy_policy_id=binding_policy_id
        )
    if has_subscription:
        UptimeCheckTaskSubscription.objects.create(uptimecheck_id=task.pk, subscription_id=1, bk_biz_id=2)

    with patch(f"{MODULE}.nodeman_v3_enabled", return_value=switch):
        assert use_deploy_policy(task.pk) is expected


@patch("bk_monitor_base.domains.uptime_check.services.task_manager.create_subscription")
def test_deploy_declares_one_policy_with_subscription_context(
    mock_create_subscription: Mock, task, policies, v3_enabled
):
    """下发：一个任务一条策略，模板名与上下文和 V2 订阅步骤一致，跨业务节点交给策略按业务拆分。"""
    result = TaskManager(task=task).deploy()

    assert result == "success"
    mock_create_subscription.assert_not_called()

    declaration: SubConfigDeclaration = policies.save.call_args.args[1]
    assert policies.save.call_args.args[0] == DEFAULT_TENANT_ID
    assert policies.save.call_args.args[2] is None  # 首次下发没有已绑定的策略
    assert declaration.name == f"bkmonitor-uptimecheck-{task.pk}"
    assert declaration.plugin_name == "bkmonitorbeat"
    assert declaration.template_name == "bkmonitorbeat_http.conf"
    assert sorted(declaration.bk_host_ids) == [101, 202]

    # 与 V2 订阅步骤的 context 逐字段一致
    expected_step = SubscriptionService(bk_biz_id=task.bk_biz_id).generate_subscription_config(
        task_id=task.pk,
        protocol=task.protocol,
        config=task.config,
        nodes=[{"bk_biz_id": 2, "bk_host_id": 101}],
        data_id=DATA_ID,
        labels=task.labels,
        task_group_id="0",
        use_custom_report=False,
    )[0]["steps"][0]
    assert declaration.context == expected_step["params"]["context"]
    assert declaration.template_name == expected_step["config"]["config_templates"][0]["name"]

    policies.execute.assert_called_once_with(DEFAULT_TENANT_ID, 88)
    binding = UptimeCheckTaskDeployPolicy.objects.get(uptimecheck_id=task.pk)
    assert binding.deploy_policy_id == 88
    assert binding.last_workflow_id == "wf-1"
    assert binding.bk_host_ids == [101, 202]
    assert binding.ensure_absent is False
    assert _reload(task).status == UptimeCheckTaskStatus.STARTING.value


def test_deploy_fails_when_node_host_id_missing(task, policies, v3_enabled):
    """只有 IP 的节点回填不到主机 ID 时不下发，任务标记为启动失败。"""
    UptimeCheckNodeModel.objects.filter(bk_host_id=202).update(bk_host_id=None)

    with patch.object(UptimeCheckNodeModel, "set_host_id", return_value=None):
        with pytest.raises(DeployTaskError):
            TaskManager(task=_reload(task)).deploy()

    policies.save.assert_not_called()
    assert _reload(task).status == UptimeCheckTaskStatus.START_FAILED.value


def test_stop_start_delete_by_policy(task, policies, v3_enabled):
    """停止、删除切换为反相并执行；启动恢复正向并按当前配置重新下发。"""
    TaskManager(task=task).deploy()

    # 停止
    TaskManager(task=_reload(task)).stop(operator="admin")
    policies.ensure_absent.assert_called_once_with(DEFAULT_TENANT_ID, 88)
    binding = UptimeCheckTaskDeployPolicy.objects.get(uptimecheck_id=task.pk)
    assert binding.ensure_absent is True
    assert binding.last_workflow_id == "wf-2"
    assert _reload(task).status == UptimeCheckTaskStatus.STOPED.value

    # 启动：复用已绑定的策略
    TaskManager(task=_reload(task)).start(operator="admin")
    assert policies.save.call_args.args[2] == 88
    binding.refresh_from_db()
    assert binding.ensure_absent is False
    assert binding.last_workflow_id == "wf-3"
    assert _reload(task).status == UptimeCheckTaskStatus.STARTING.value

    # 删除：策略保留为反相
    TaskManager(task=_reload(task)).delete(operator="admin")
    assert policies.ensure_absent.call_count == 2
    binding.refresh_from_db()
    assert binding.ensure_absent is True
    assert binding.last_workflow_id == "wf-4"


@pytest.mark.parametrize("state", [DeployState.DISPATCHING, DeployState.RUNNING])
def test_stop_blocked_while_last_execution_running(task, policies, v3_enabled, state):
    """最近一次执行未结束时不允许启停，与订阅的前置检查一致。"""
    TaskManager(task=task).deploy()
    policies.execution_state.return_value = ExecutionState(state)

    with pytest.raises(DeployTaskError):
        TaskManager(task=_reload(task)).stop(operator="admin")

    policies.ensure_absent.assert_not_called()
    assert _reload(task).status == UptimeCheckTaskStatus.STARTING.value


@pytest.mark.parametrize(
    "state,expected_status",
    [
        (ExecutionState(DeployState.DISPATCHING), UptimeCheckTaskStatus.STARTING.value),
        (ExecutionState(DeployState.RUNNING), UptimeCheckTaskStatus.STARTING.value),
        (ExecutionState(DeployState.SUCCESS), UptimeCheckTaskStatus.RUNNING.value),
        (
            ExecutionState(
                DeployState.FAILED,
                [
                    HostDeployState(101, DeployState.FAILED, "inst-101"),
                    HostDeployState(202, DeployState.SUCCESS, "inst-202"),
                ],
            ),
            UptimeCheckTaskStatus.START_FAILED.value,
        ),
    ],
)
def test_refresh_task_status_by_policy(task, policies, v3_enabled, state, expected_status):
    """状态刷新按最近一次执行汇总，执行记录取自绑定。"""
    TaskManager(task=task).deploy()
    policies.execution_state.return_value = state
    policies.failure_logs.return_value = ["[渲染配置] 模板渲染失败"]

    result = refresh_task_status(bk_tenant_id="default", bk_biz_id=2, task_ids=[task.pk])

    assert result == {task.pk: expected_status}
    assert _reload(task).status == expected_status
    policies.execution_state.assert_called_with(DEFAULT_TENANT_ID, "wf-1", [101, 202])


def test_refresh_records_failure_logs_once(task, policies, v3_enabled):
    """失败时按节点记录日志；重复刷新不重复记录，重新下发后清空旧日志。"""
    TaskManager(task=task).deploy()
    policies.execution_state.return_value = ExecutionState(
        DeployState.FAILED, [HostDeployState(101, DeployState.FAILED, "inst-101")]
    )
    policies.failure_logs.return_value = ["[渲染配置] 模板渲染失败"]

    refresh_task_status(bk_tenant_id="default", bk_biz_id=2, task_ids=[task.pk])
    refresh_task_status(bk_tenant_id="default", bk_biz_id=2, task_ids=[task.pk])

    logs = list(UptimeCheckTaskCollectorLog.objects.filter(task_id=task.pk, is_deleted=False))
    assert [log.error_log for log in logs] == ["本业务节点: [渲染配置] 模板渲染失败"]
    policies.failure_logs.assert_called_once_with(DEFAULT_TENANT_ID, "inst-101")

    TaskManager(task=_reload(task)).deploy()
    assert not UptimeCheckTaskCollectorLog.objects.filter(task_id=task.pk, is_deleted=False).exists()


def test_refresh_ignores_absent_policy(task, policies, v3_enabled):
    """停止后最近一次执行是反相，不据此改任务状态。"""
    TaskManager(task=task).deploy()
    TaskManager(task=_reload(task)).stop(operator="admin")
    policies.execution_state.reset_mock()

    assert refresh_task_status(bk_tenant_id="default", bk_biz_id=2, task_ids=[task.pk]) == {}
    policies.execution_state.assert_not_called()
    assert _reload(task).status == UptimeCheckTaskStatus.STOPED.value


@patch("bk_monitor_base.domains.uptime_check.operation.batch_get_subscription_task_result")
def test_refresh_keeps_subscription_tasks_on_v2(mock_batch_result: Mock, task, policies, v3_enabled):
    """同一批刷新里，存量订阅任务继续按订阅查询，部署策略任务不查订阅。"""
    v2_task = UptimeCheckTaskModel.objects.create(
        bk_biz_id=2, name="V2 任务", protocol=UptimeCheckTaskModel.Protocol.HTTP, status="STARTING"
    )
    UptimeCheckTaskSubscription.objects.create(uptimecheck_id=v2_task.pk, subscription_id=9527, bk_biz_id=2)
    mock_batch_result.return_value = ([{"status": "SUCCESS"}], None)
    TaskManager(task=task).deploy()

    result = refresh_task_status(bk_tenant_id="default", bk_biz_id=2, task_ids=[task.pk, v2_task.pk])

    assert result == {task.pk: UptimeCheckTaskStatus.RUNNING.value, v2_task.pk: UptimeCheckTaskStatus.RUNNING.value}
    assert [call.kwargs["params"]["subscription_id"] for call in mock_batch_result.call_args_list] == [9527]
