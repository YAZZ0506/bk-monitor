"""共享进程子配置的部署策略能力（bkmonitorbeat 等）。

一份采集项对应一条 NodeMan V3 部署策略（``specify_plugin_sub_config_template``），由 NodeMan 收敛：
新增、更新、范围收缩都通过正向声明 + 执行完成；停用和删除不改范围，把策略切换为反相
（``ensure_absent=true``）并执行，NodeMan 持续确保该策略生成的子配置不存在。NodeMan 不提供
删除策略的接口，策略在采集项删除后保留为反相。

本模块不保存状态，策略 ID、最近一次 workflow_id、下发主机由调用方各自落库。
只有 V3 实现，调用方自行判断是否走部署策略。
"""

import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, cast

V3Request = Callable[[str, str, dict[str, Any]], Any]

SPEC_TYPE = "specify_plugin_sub_config_template"
SCOPE_TYPE = "instance"
SCOPE_GRANULARITY = "host"
PAGE_SIZE = 500

# 插件操作的生命周期状态（PluginWorkflowOperationList）；init/launched/running 及未知状态都视为执行中
_OPERATION_SUCCESS_STATES = {"success"}
_OPERATION_FAILED_STATES = {"failed", "timeout", "terminated"}
# 部署策略父任务状态：只表示派发，不汇总子任务
_PARENT_FAILED_STATES = {"failed", "partial_failed"}
_LOG_ERROR_LEVELS = {"error", "warn", "warning", "fatal"}


class SubConfigPolicyError(Exception):
    """部署策略声明或执行失败。"""


class DeployState(StrEnum):
    """一次执行在监控侧的收敛状态。"""

    DISPATCHING = "dispatching"
    """父任务仍在派发，或节点管理尚未返回记录。"""
    RUNNING = "running"
    """派发完成，仍有主机在执行。"""
    SUCCESS = "success"
    """本次涉及的主机全部完成（包括无需变更）。"""
    FAILED = "failed"
    """派发失败，或至少一台主机执行失败。"""


_STATE_PRIORITY = {DeployState.SUCCESS: 0, DeployState.RUNNING: 1, DeployState.FAILED: 2}


@dataclass(frozen=True)
class SubConfigDeclaration:
    """一份子配置的正向声明。

    Attributes:
        name: 策略名称，同一租户内唯一，用于绑定丢失时按名称找回。
        description: 策略描述。
        plugin_name: 承载子配置的插件，如 ``bkmonitorbeat``。
        template_name: 插件包中声明的子配置模板名，如 ``bkmonitorbeat_http.conf``。
        context: 渲染模板的 custom context，与 V2 订阅步骤的 context 一致。
        bk_host_ids: 下发主机。
    """

    name: str
    description: str
    plugin_name: str
    template_name: str
    context: dict[str, Any]
    bk_host_ids: list[int]


@dataclass(frozen=True)
class HostDeployState:
    """单台主机在一次执行中的状态。

    Attributes:
        bk_host_id: 主机 ID。
        state: 收敛状态。
        oper_inst_id: 最近一次操作实例 ID，用于查询失败日志；无操作时为空。
    """

    bk_host_id: int
    state: DeployState
    oper_inst_id: str = ""


@dataclass(frozen=True)
class ExecutionState:
    """一次执行的汇总状态。

    Attributes:
        state: 汇总状态，任一主机失败即为失败。
        hosts: 有执行记录的主机；不在其中的主机表示本次无需变更。
        message: 派发失败等无法归到主机的原因。
    """

    state: DeployState
    hosts: list[HostDeployState] = field(default_factory=list)
    message: str = ""


def _unique_host_ids(values: Iterable[Any]) -> list[int]:
    """去掉空值与 0 并去重排序。"""
    return sorted({int(value) for value in values if value not in (None, "")} - {0})


def _chunks(values: list[int], size: int = PAGE_SIZE) -> Iterable[list[int]]:
    for start in range(0, len(values), size):
        yield values[start : start + size]


def _worse(current: DeployState | None, new: DeployState) -> DeployState:
    """同一主机出现在多个子任务时取更差的状态。"""
    if current is None:
        return new
    return new if _STATE_PRIORITY[new] > _STATE_PRIORITY[current] else current


def _dict(value: Any) -> dict[str, Any]:
    """接口返回的对象；缺失或类型不符时按空对象处理。"""
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def _values(value: Any) -> list[Any]:
    """接口返回的数组；缺失或类型不符时按空数组处理。"""
    return cast(list[Any], value) if isinstance(value, list) else []


def _dicts(value: Any) -> list[dict[str, Any]]:
    """接口返回的对象列表；忽略非对象元素。"""
    return [cast(dict[str, Any], item) for item in _values(value) if isinstance(item, dict)]


class SubConfigPolicies:
    """基于 NodeMan V3 部署策略管理共享进程的子配置。"""

    def __init__(self, request: V3Request) -> None:
        self.request: V3Request = request

    # ------------------------------------------------------------------ 声明
    def save(self, bk_tenant_id: str, declaration: SubConfigDeclaration, deploy_policy_id: int | None) -> int:
        """创建或更新为正向声明，不触发执行。

        调用方应先落库返回的策略 ID 再执行，避免执行失败后重复创建同名策略。

        Args:
            bk_tenant_id: 租户 ID。
            declaration: 子配置声明。
            deploy_policy_id: 已绑定的策略 ID；为空时先按名称找回，找不到再创建。

        Returns:
            策略 ID。

        Raises:
            SubConfigPolicyError: 没有主机、主机在节点管理中不存在、同名策略不唯一或接口未返回 ID。
        """
        host_ids = _unique_host_ids(declaration.bk_host_ids)
        if not host_ids:
            raise SubConfigPolicyError("no target hosts")
        scopes = self._build_scopes(bk_tenant_id, host_ids)
        specs = self._build_specs(declaration)

        if not deploy_policy_id:
            deploy_policy_id = self.find_policy_id(bk_tenant_id, declaration.name)

        if deploy_policy_id:
            self.request(
                "update_deploy_policy",
                bk_tenant_id,
                {
                    "deploy_policies": [
                        {
                            "deploy_policy_id": deploy_policy_id,
                            "meta": {"name": declaration.name, "description": declaration.description},
                            "specs": specs,
                            "scopes": scopes,
                            "enabled": True,
                            "ensure_absent": False,
                        }
                    ],
                    "fields": {"meta": True, "specs": True, "scopes": True, "enabled": True, "ensure_absent": True},
                },
            )
            return deploy_policy_id

        result = self.request(
            "create_deploy_policy",
            bk_tenant_id,
            {
                "name": declaration.name,
                "description": declaration.description,
                "enabled": True,
                "specs": specs,
                "scopes": scopes,
            },
        )
        created_id = int(_dict(result).get("deploy_policy_id") or 0)
        if not created_id:
            raise SubConfigPolicyError(f"create deploy policy {declaration.name} returned no deploy_policy_id")
        return created_id

    def ensure_absent(self, bk_tenant_id: str, deploy_policy_id: int) -> None:
        """切换为反相声明，不改范围和规格，不触发执行。"""
        self.request(
            "update_deploy_policy",
            bk_tenant_id,
            {
                "deploy_policies": [{"deploy_policy_id": deploy_policy_id, "ensure_absent": True}],
                "fields": {"meta": False, "specs": False, "scopes": False, "enabled": False, "ensure_absent": True},
            },
        )

    def execute(self, bk_tenant_id: str, deploy_policy_id: int) -> str:
        """执行一次策略，返回本次执行的父 workflow_id。

        Raises:
            SubConfigPolicyError: 接口未返回 workflow_id。
        """
        result = self.request("execute_deploy_policy", bk_tenant_id, {"deploy_policy_id": deploy_policy_id})
        workflow_id = str(_dict(result).get("workflow_id") or "")
        if not workflow_id:
            raise SubConfigPolicyError(f"execute deploy policy({deploy_policy_id}) returned no workflow_id")
        return workflow_id

    def find_policy_id(self, bk_tenant_id: str, name: str) -> int | None:
        """按名称查找策略。

        Raises:
            SubConfigPolicyError: 同名策略不唯一。
        """
        matches: set[int] = set()
        offset = 0
        while True:
            data = self.request(
                "list_deploy_policies",
                bk_tenant_id,
                {
                    "page": {"offset": offset, "limit": PAGE_SIZE},
                    "exact_include_conditions": {"deploy_policy_name": [name]},
                },
            )
            items = _dicts(_dict(data).get("items"))
            for item in items:
                item_name = _dict(item.get("meta")).get("name") or item.get("name")
                policy_id = int(item.get("deploy_policy_id") or 0)
                if item_name == name and policy_id:
                    matches.add(policy_id)
            if len(items) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
        if len(matches) > 1:
            raise SubConfigPolicyError(f"found multiple deploy policies named {name}: {sorted(matches)}")
        return next(iter(matches)) if matches else None

    @staticmethod
    def _build_specs(declaration: SubConfigDeclaration) -> list[dict[str, Any]]:
        # JSON 往返：与节点管理存储后的形态一致（如整型字典键变为字符串），避免每次比对都判定为变化
        context = json.loads(json.dumps(declaration.context, default=str))
        return [
            {
                "type": SPEC_TYPE,
                "param": {
                    "plugin_name": declaration.plugin_name,
                    "config_files_detail": [{"template_name": declaration.template_name, "is_main_config": False}],
                    "custom_config_context": context,
                },
            }
        ]

    def _build_scopes(self, bk_tenant_id: str, host_ids: list[int]) -> list[dict[str, Any]]:
        """instance 范围要求带主机在节点管理中所属的业务，按业务分组。

        Raises:
            SubConfigPolicyError: 有主机在节点管理中查不到；不做部分下发，交由调用方提示用户。
        """
        host_biz = self._host_biz_ids(bk_tenant_id, host_ids)
        missing = [host_id for host_id in host_ids if host_id not in host_biz]
        if missing:
            raise SubConfigPolicyError(f"hosts not found in nodeman: {missing}")

        biz_hosts: dict[int, list[int]] = {}
        for host_id in host_ids:
            biz_hosts.setdefault(host_biz[host_id], []).append(host_id)
        return [
            {
                "type": SCOPE_TYPE,
                "scope": {
                    "granularity": SCOPE_GRANULARITY,
                    "bk_biz_id": bk_biz_id,
                    "instance_ids": biz_hosts[bk_biz_id],
                },
            }
            for bk_biz_id in sorted(biz_hosts)
        ]

    def _host_biz_ids(self, bk_tenant_id: str, host_ids: list[int]) -> dict[int, int]:
        result: dict[int, int] = {}
        for batch in _chunks(host_ids):
            offset = 0
            while True:
                data = self.request(
                    "list_hosts",
                    bk_tenant_id,
                    {
                        "page": {"offset": offset, "limit": PAGE_SIZE},
                        "exact_include_conditions": {"bk_host_id": batch},
                    },
                )
                items = _dicts(_dict(data).get("items"))
                for item in items:
                    result.setdefault(int(item["bk_host_id"]), int(_dict(item.get("info"))["bk_biz_id"]))
                offset += len(items)
                if not items or offset >= int(_dict(data).get("total") or 0):
                    break
        return result

    # ------------------------------------------------------------------ 状态
    def execution_state(self, bk_tenant_id: str, workflow_id: str, bk_host_ids: Iterable[Any]) -> ExecutionState:
        """汇总一次执行在指定主机上的结果。

        父任务只表示派发；派发完成后按子插件任务里的操作判断主机结果。子任务可能与同机其他策略共用，
        只统计调用方关心的主机。主机没有任何操作表示本次无需变更。

        Args:
            bk_tenant_id: 租户 ID。
            workflow_id: ``execute`` 返回的父 workflow_id。
            bk_host_ids: 本次下发的主机。
        """
        host_ids = set(_unique_host_ids(bk_host_ids))
        data = self.request(
            "list_deploy_policy_workflows",
            bk_tenant_id,
            {"page": {"offset": 0, "limit": 1}, "exact_include_conditions": {"workflow_id": [workflow_id]}},
        )
        items = _dicts(_dict(data).get("items"))
        if not items:
            # 刚执行时记录可能尚未可见，不据此推断完成
            return ExecutionState(DeployState.DISPATCHING)
        parent = items[0]
        parent_status = str(parent.get("status") or "")
        if parent_status in _PARENT_FAILED_STATES:
            return ExecutionState(DeployState.FAILED, message=f"deploy policy dispatch {parent_status}")
        if parent_status != "success":
            # running 或无法识别的状态：继续等待
            return ExecutionState(DeployState.DISPATCHING)

        child_ids = [
            str(child["workflow_id"])
            for child in _dicts(parent.get("children"))
            if child.get("type") == "plugin" and child.get("workflow_id")
        ]
        if not child_ids or not host_ids:
            return ExecutionState(DeployState.SUCCESS)

        child_status = self._plugin_workflow_status(bk_tenant_id, child_ids)
        host_states: dict[int, DeployState] = {}
        host_oper_inst: dict[int, str] = {}
        for child_id in child_ids:
            status, child_hosts = child_status.get(child_id, ("running", set()))
            operations = self._plugin_operations(bk_tenant_id, child_id, sorted(host_ids))
            seen: set[int] = set()
            for operation in operations:
                host_id = int(_dict(operation.get("plugin_deployment_info")).get("bk_host_id") or 0)
                if host_id not in host_ids:
                    continue
                seen.add(host_id)
                life_cycle = _dict(_dict(operation.get("latest_oper_inst_brief_data")).get("life_cycle"))
                state = self._operation_state(str(life_cycle.get("state") or ""))
                host_states[host_id] = _worse(host_states.get(host_id), state)
                instance_ids = [str(value) for value in _values(operation.get("instance_ids")) if value]
                if instance_ids and state is DeployState.FAILED:
                    host_oper_inst[host_id] = instance_ids[-1]
                elif instance_ids:
                    host_oper_inst.setdefault(host_id, instance_ids[-1])
            # 子任务仍在运行但主机的操作还没有生成，不能当作无需变更
            if status == "running":
                for host_id in (child_hosts & host_ids) - seen:
                    host_states[host_id] = _worse(host_states.get(host_id), DeployState.RUNNING)

        hosts = [
            HostDeployState(bk_host_id=host_id, state=state, oper_inst_id=host_oper_inst.get(host_id, ""))
            for host_id, state in sorted(host_states.items())
        ]
        if any(host.state is DeployState.FAILED for host in hosts):
            return ExecutionState(DeployState.FAILED, hosts)
        if any(host.state is DeployState.RUNNING for host in hosts):
            return ExecutionState(DeployState.RUNNING, hosts)
        return ExecutionState(DeployState.SUCCESS, hosts)

    def failure_logs(self, bk_tenant_id: str, oper_inst_id: str) -> list[str]:
        """提取操作实例中失败步骤的日志，优先错误级别，没有时保留失败步骤的全部日志。"""
        if not oper_inst_id:
            return []
        data = _dict(
            self.request("get_plugin_workflow_operation_instance_log", bk_tenant_id, {"oper_inst_id": oper_inst_id})
        )
        lines: list[str] = []
        for action in _dict(data.get("oper_inst_logs")).values():
            action = _dict(action)
            state = str(_dict(action.get("life_cycle")).get("state") or "")
            if state not in _OPERATION_FAILED_STATES:
                continue
            name = str(action.get("display_name_zh") or action.get("display_name_en") or "")
            logs = _dicts(_dict(action.get("message")).get("logs"))
            selected = [log for log in logs if str(log.get("level") or "").lower() in _LOG_ERROR_LEVELS] or logs
            lines.extend(f"[{name}] {log.get('text_zh') or log.get('text_en') or ''}".strip() for log in selected)
        extra = _dicts(_dict(data.get("extra_execution_logs")).get("logs"))
        lines.extend(
            str(log.get("text_zh") or log.get("text_en") or "")
            for log in extra
            if str(log.get("level") or "").lower() in _LOG_ERROR_LEVELS
        )
        return [line for line in lines if line]

    @staticmethod
    def _operation_state(state: str) -> DeployState:
        if state in _OPERATION_SUCCESS_STATES:
            return DeployState.SUCCESS
        if state in _OPERATION_FAILED_STATES:
            return DeployState.FAILED
        # init/launched/running 以及无法识别的状态都继续等待，不推断成功
        return DeployState.RUNNING

    def _plugin_workflow_status(self, bk_tenant_id: str, workflow_ids: list[str]) -> dict[str, tuple[str, set[int]]]:
        result: dict[str, tuple[str, set[int]]] = {}
        for start in range(0, len(workflow_ids), PAGE_SIZE):
            batch = workflow_ids[start : start + PAGE_SIZE]
            data = self.request(
                "list_plugin_workflows",
                bk_tenant_id,
                {"page": {"offset": 0, "limit": PAGE_SIZE}, "exact_include_conditions": {"workflow_id": batch}},
            )
            for item in _dicts(_dict(data).get("items")):
                hosts = {int(host_id) for host_id in _values(item.get("bk_host_id")) if host_id}
                result[str(item["workflow_id"])] = (str(item.get("status") or ""), hosts)
        return result

    def _plugin_operations(self, bk_tenant_id: str, workflow_id: str, host_ids: list[int]) -> list[dict[str, Any]]:
        operations: list[dict[str, Any]] = []
        for batch in _chunks(host_ids):
            offset = 0
            while True:
                data = self.request(
                    "list_plugin_workflow_operations",
                    bk_tenant_id,
                    {
                        "page": {"offset": offset, "limit": PAGE_SIZE},
                        "workflow_id": workflow_id,
                        "exact_include_conditions": {"bk_host_id": batch},
                    },
                )
                batch_operations = _dicts(_dict(data).get("operations"))
                operations.extend(batch_operations)
                offset += len(batch_operations)
                if not batch_operations or offset >= int(_dict(data).get("total") or 0):
                    break
        return operations
