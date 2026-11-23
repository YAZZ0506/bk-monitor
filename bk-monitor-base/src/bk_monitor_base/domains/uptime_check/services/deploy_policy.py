"""
拨测任务的节点管理 V3 部署策略

一个拨测任务对应一条 bkmonitorbeat 子配置部署策略：
- 下发、编辑、启动：正向声明（模板名与上下文与 V2 订阅一致）并执行；
- 停止、删除：切换为反相并执行，策略保留；
- 状态：按最近一次执行的子插件任务汇总任务节点的结果。

已有该绑定的任务始终走部署策略；已有 V2 订阅的存量任务继续走订阅；新任务在 V3 开启时走部署策略。
"""

import logging
from typing import Any, final

from bk_monitor_base.domains.uptime_check.constants import UPTIME_CHECK_BEAT_PLUGIN_NAME
from bk_monitor_base.domains.uptime_check.define import UptimeCheckTaskStatus
from bk_monitor_base.domains.uptime_check.models import (
    UptimeCheckTaskCollectorLog,
    UptimeCheckTaskDeployPolicy,
    UptimeCheckTaskModel,
    UptimeCheckTaskSubscription,
)
from bk_monitor_base.infras.nodeman_control import (
    DeployState,
    ExecutionState,
    SubConfigDeclaration,
    SubConfigPolicies,
    nodeman_v3_enabled,
    sub_config_policies,
)

logger = logging.getLogger(__name__)

POLICY_NAME_PREFIX = "bkmonitor-uptimecheck"


def use_deploy_policy(task_id: int) -> bool:
    """判断拨测任务是否走部署策略。

    已创建过部署策略的任务始终走部署策略；已有 V2 订阅的存量任务继续走订阅，不在这里迁移；
    其余任务按 V3 开关决定（首次下发在创建策略前失败、只留下空绑定的任务也按开关决定）。
    """
    if UptimeCheckTaskDeployPolicy.objects.filter(uptimecheck_id=task_id, deploy_policy_id__isnull=False).exists():
        return True
    if UptimeCheckTaskSubscription.objects.filter(uptimecheck_id=task_id, is_deleted=False).exists():
        return False
    return nodeman_v3_enabled()


@final
class UptimeCheckDeployPolicy:
    """拨测任务与部署策略的绑定及操作。"""

    def __init__(
        self, task: UptimeCheckTaskModel, bk_tenant_id: str, policies: SubConfigPolicies | None = None
    ) -> None:
        """
        Args:
            task: 拨测任务
            bk_tenant_id: 租户ID
            policies: 部署策略能力；默认使用节点管理 V3 实现
        """
        self.task = task
        self.bk_tenant_id = bk_tenant_id
        self.policies = policies or sub_config_policies

    @staticmethod
    def policy_name(task_id: int) -> str:
        """部署策略名称，同一租户内按任务唯一，用于绑定丢失时找回策略。"""
        return f"{POLICY_NAME_PREFIX}-{task_id}"

    def binding(self) -> UptimeCheckTaskDeployPolicy | None:
        """当前任务的部署策略绑定。"""
        return UptimeCheckTaskDeployPolicy.objects.filter(uptimecheck_id=self.task.pk).first()

    def deploy(self, template_name: str, context: dict[str, Any], bk_host_ids: list[int]) -> str:
        """正向声明并执行。

        策略 ID 在执行前落库，执行失败时下次操作复用同一条策略。

        Args:
            template_name: 子配置模板名
            context: 渲染上下文
            bk_host_ids: 拨测节点主机

        Returns:
            本次执行的 workflow_id

        Raises:
            SubConfigPolicyError: 声明或执行失败
        """
        record, _ = UptimeCheckTaskDeployPolicy.objects.get_or_create(
            uptimecheck_id=self.task.pk,
            defaults={"bk_tenant_id": self.bk_tenant_id, "policy_name": self.policy_name(self.task.pk)},
        )
        declaration = SubConfigDeclaration(
            name=record.policy_name,
            description=f"bkmonitor uptime check task {self.task.pk} ({self.task.protocol})",
            plugin_name=UPTIME_CHECK_BEAT_PLUGIN_NAME,
            template_name=template_name,
            context=context,
            bk_host_ids=bk_host_ids,
        )
        deploy_policy_id = self.policies.save(self.bk_tenant_id, declaration, record.deploy_policy_id)
        record.deploy_policy_id = deploy_policy_id
        record.ensure_absent = False
        record.save(update_fields=["deploy_policy_id", "ensure_absent", "update_time"])

        workflow_id = self.policies.execute(self.bk_tenant_id, deploy_policy_id)
        record.last_workflow_id = workflow_id
        record.bk_host_ids = sorted(set(bk_host_ids))
        record.save(update_fields=["last_workflow_id", "bk_host_ids", "update_time"])
        logger.info(
            "uptime check task(%s) deploy policy(%s) executed, workflow_id=%s, hosts=%s",
            self.task.pk,
            deploy_policy_id,
            workflow_id,
            record.bk_host_ids,
        )
        return workflow_id

    def withdraw(self) -> str:
        """切换为反相并执行，确保各节点上的子配置被删除；从未创建过策略时无需处理。

        Returns:
            本次执行的 workflow_id；没有策略时为空字符串
        """
        record = self.binding()
        if not record or not record.deploy_policy_id:
            return ""
        self.policies.ensure_absent(self.bk_tenant_id, record.deploy_policy_id)
        record.ensure_absent = True
        record.save(update_fields=["ensure_absent", "update_time"])

        workflow_id = self.policies.execute(self.bk_tenant_id, record.deploy_policy_id)
        record.last_workflow_id = workflow_id
        record.save(update_fields=["last_workflow_id", "update_time"])
        logger.info(
            "uptime check task(%s) deploy policy(%s) withdrawn, workflow_id=%s",
            self.task.pk,
            record.deploy_policy_id,
            workflow_id,
        )
        return workflow_id

    def execution_state(self) -> ExecutionState | None:
        """最近一次执行的状态；没有执行记录时返回 None。"""
        record = self.binding()
        if not record or not record.last_workflow_id:
            return None
        return self.policies.execution_state(self.bk_tenant_id, record.last_workflow_id, record.bk_host_ids)

    def refresh_status(self) -> str | None:
        """按最近一次正向执行的结果推导任务状态，失败时记录节点日志。

        反相（停止、删除）后的执行不改变任务状态，返回 None。

        Returns:
            任务状态；无需更新时返回 None
        """
        record = self.binding()
        if not record or record.ensure_absent or not record.last_workflow_id:
            return None
        state = self.policies.execution_state(self.bk_tenant_id, record.last_workflow_id, record.bk_host_ids)
        if state.state in (DeployState.DISPATCHING, DeployState.RUNNING):
            return UptimeCheckTaskStatus.STARTING.value
        if state.state is DeployState.SUCCESS:
            return UptimeCheckTaskStatus.RUNNING.value
        self._record_failure_logs(state)
        return UptimeCheckTaskStatus.START_FAILED.value

    def _record_failure_logs(self, state: ExecutionState) -> None:
        """记录失败节点的日志；同一次下发只记录一次（下发时会清空旧日志）。"""
        if UptimeCheckTaskCollectorLog.objects.filter(task_id=self.task.pk, is_deleted=False).exists():
            return

        node_names = {node.bk_host_id: node.name for node in self.task.nodes.all() if node.bk_host_id}
        messages: list[str] = []
        if state.message:
            messages.append(state.message)
        for host in state.hosts:
            if host.state is not DeployState.FAILED:
                continue
            node = node_names.get(host.bk_host_id) or str(host.bk_host_id)
            try:
                lines = self.policies.failure_logs(self.bk_tenant_id, host.oper_inst_id)
            except Exception as e:  # 日志查询失败不影响状态更新
                logger.warning("uptime check task(%s) fetch failure logs failed: %s", self.task.pk, e)
                lines = []
            messages.extend(f"{node}: {line}" for line in lines or ["deploy failed"])

        UptimeCheckTaskCollectorLog.objects.bulk_create(
            [UptimeCheckTaskCollectorLog(task_id=self.task.pk, error_log=message) for message in messages]
        )
