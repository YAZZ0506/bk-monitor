"""共享进程子配置部署策略能力的测试。

只用 NodeMan V3 原生响应结构驱动 ``SubConfigPolicies``，校验请求载荷与状态汇总语义，不依赖网络。
"""

from typing import Any
from unittest.mock import Mock

import pytest

from bk_monitor_base.infras.nodeman_control.sub_config_policy import (
    DeployState,
    SubConfigDeclaration,
    SubConfigPolicies,
    SubConfigPolicyError,
)

TENANT = "system"


def declaration(**overrides: Any) -> SubConfigDeclaration:
    """构造一份默认的子配置声明。"""
    values: dict[str, Any] = {
        "name": "bkmonitor-uptimecheck-1",
        "description": "uptime check 1",
        "plugin_name": "bkmonitorbeat",
        "template_name": "bkmonitorbeat_http.conf",
        "context": {"data_id": 1011, "labels": {1: "a"}},
        "bk_host_ids": [3, 1, 2, 1, 0, None],
    }
    values.update(overrides)
    return SubConfigDeclaration(**values)


def host(host_id: int, biz: int) -> dict[str, Any]:
    """节点管理主机列表中的一项。"""
    return {"bk_host_id": str(host_id), "info": {"bk_biz_id": str(biz)}}


class FakeNodeMan:
    """按 action 返回预置响应，并记录每次请求。"""

    def __init__(self, responses: dict[str, Any]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def __call__(self, action: str, tenant: str, params: dict[str, Any]) -> Any:
        assert tenant == TENANT
        self.calls.append((action, params))
        response = self.responses.get(action)
        return response(params) if callable(response) else response

    def params(self, action: str) -> list[dict[str, Any]]:
        return [params for called, params in self.calls if called == action]


def test_save_creates_policy_with_scopes_grouped_by_nodeman_biz():
    """没有绑定且按名称找不到时创建策略：主机去重排序，按节点管理中的业务拆范围，context 做 JSON 往返。"""
    nodeman = FakeNodeMan(
        {
            "list_hosts": {"total": 3, "items": [host(1, 2), host(2, 5), host(3, 2)]},
            "list_deploy_policies": {"total": 0, "items": []},
            "create_deploy_policy": {"deploy_policy_id": 88},
        }
    )

    policy_id = SubConfigPolicies(nodeman).save(TENANT, declaration(), None)

    assert policy_id == 88
    assert nodeman.params("list_hosts")[0]["exact_include_conditions"] == {"bk_host_id": [1, 2, 3]}
    created = nodeman.params("create_deploy_policy")[0]
    assert created["name"] == "bkmonitor-uptimecheck-1"
    assert created["enabled"] is True
    assert created["scopes"] == [
        {"type": "instance", "scope": {"granularity": "host", "bk_biz_id": 2, "instance_ids": [1, 3]}},
        {"type": "instance", "scope": {"granularity": "host", "bk_biz_id": 5, "instance_ids": [2]}},
    ]
    assert created["specs"] == [
        {
            "type": "specify_plugin_sub_config_template",
            "param": {
                "plugin_name": "bkmonitorbeat",
                "config_files_detail": [{"template_name": "bkmonitorbeat_http.conf", "is_main_config": False}],
                # 整型字典键与节点管理存储后的形态一致
                "custom_config_context": {"data_id": 1011, "labels": {"1": "a"}},
            },
        }
    ]
    assert not nodeman.params("update_deploy_policy")


@pytest.mark.parametrize(
    "bound_id,remote_items,expected_list_calls",
    [
        # 已绑定：直接更新，不按名称查找
        (66, [], 0),
        # 绑定丢失：按名称找回后更新
        (None, [{"deploy_policy_id": 66, "meta": {"name": "bkmonitor-uptimecheck-1"}}], 1),
    ],
)
def test_save_updates_existing_policy_back_to_forward(bound_id, remote_items, expected_list_calls):
    """已有策略时更新为正向声明，ensure_absent 显式改回 false，停用后重新启用也走这里。"""
    nodeman = FakeNodeMan(
        {
            "list_hosts": {"total": 1, "items": [host(1, 2)]},
            "list_deploy_policies": {"total": len(remote_items), "items": remote_items},
        }
    )

    policy_id = SubConfigPolicies(nodeman).save(TENANT, declaration(bk_host_ids=[1]), bound_id)

    assert policy_id == 66
    assert len(nodeman.params("list_deploy_policies")) == expected_list_calls
    update = nodeman.params("update_deploy_policy")[0]
    assert update["fields"] == {"meta": True, "specs": True, "scopes": True, "enabled": True, "ensure_absent": True}
    policy = update["deploy_policies"][0]
    assert policy["deploy_policy_id"] == 66
    assert policy["ensure_absent"] is False
    assert policy["enabled"] is True
    assert policy["meta"] == {"name": "bkmonitor-uptimecheck-1", "description": "uptime check 1"}
    assert not nodeman.params("create_deploy_policy")


@pytest.mark.parametrize(
    "responses,decl,error",
    [
        # 没有主机不提交
        ({}, declaration(bk_host_ids=[0, None]), "no target hosts"),
        # 有主机在节点管理中查不到时不做部分下发
        ({"list_hosts": {"total": 1, "items": [host(1, 2)]}}, declaration(bk_host_ids=[1, 9]), "hosts not found"),
        # 同名策略不唯一时不猜测
        (
            {
                "list_hosts": {"total": 1, "items": [host(1, 2)]},
                "list_deploy_policies": {
                    "total": 2,
                    "items": [
                        {"deploy_policy_id": 1, "meta": {"name": "bkmonitor-uptimecheck-1"}},
                        {"deploy_policy_id": 2, "meta": {"name": "bkmonitor-uptimecheck-1"}},
                    ],
                },
            },
            declaration(bk_host_ids=[1]),
            "multiple deploy policies",
        ),
        # 创建接口没有返回 ID
        (
            {
                "list_hosts": {"total": 1, "items": [host(1, 2)]},
                "list_deploy_policies": {"total": 0, "items": []},
                "create_deploy_policy": {},
            },
            declaration(bk_host_ids=[1]),
            "returned no deploy_policy_id",
        ),
    ],
)
def test_save_fails_closed(responses, decl, error):
    """声明无法完整提交时直接报错，交给调用方提示用户。"""
    nodeman = FakeNodeMan(responses)
    with pytest.raises(SubConfigPolicyError, match=error):
        SubConfigPolicies(nodeman).save(TENANT, decl, None)
    assert not nodeman.params("create_deploy_policy") or error == "returned no deploy_policy_id"
    assert not nodeman.params("update_deploy_policy")


def test_ensure_absent_only_updates_direction():
    """反相只改 ensure_absent，范围、规格、启用状态都保持不变。"""
    nodeman = FakeNodeMan({})
    SubConfigPolicies(nodeman).ensure_absent(TENANT, 66)
    assert nodeman.params("update_deploy_policy") == [
        {
            "deploy_policies": [{"deploy_policy_id": 66, "ensure_absent": True}],
            "fields": {"meta": False, "specs": False, "scopes": False, "enabled": False, "ensure_absent": True},
        }
    ]


@pytest.mark.parametrize("response", [{}, None, {"workflow_id": ""}])
def test_execute_requires_workflow_id(response):
    """执行没有返回 workflow_id 时无法跟踪结果，按失败处理。"""
    nodeman = FakeNodeMan({"execute_deploy_policy": response})
    with pytest.raises(SubConfigPolicyError, match="returned no workflow_id"):
        SubConfigPolicies(nodeman).execute(TENANT, 66)


def operation(host_id: int, state: str, instance_id: str = "") -> dict[str, Any]:
    """插件任务流中的一个主机操作。"""
    return {
        "operation_id": f"op-{host_id}",
        "instance_ids": [instance_id] if instance_id else [],
        "plugin_deployment_info": {"bk_host_id": host_id, "plugin_name": "bkmonitorbeat"},
        "latest_oper_inst_brief_data": {"life_cycle": {"state": state}},
    }


def parent(status: str, *children: str) -> dict[str, Any]:
    """部署策略执行的父任务。"""
    return {
        "total": 1,
        "items": [
            {
                "workflow_id": "wf-parent",
                "status": status,
                "children": [{"type": "plugin", "workflow_id": child} for child in children],
            }
        ],
    }


@pytest.mark.parametrize(
    "case,parent_resp,child_resp,operations,expected,expected_hosts",
    [
        ("父任务尚不可见", {"total": 0, "items": []}, None, [], DeployState.DISPATCHING, {}),
        ("父任务派发中", parent("running"), None, [], DeployState.DISPATCHING, {}),
        ("父任务派发失败", parent("failed"), None, [], DeployState.FAILED, {}),
        ("父任务被终止", parent("partial_failed"), None, [], DeployState.FAILED, {}),
        ("无法识别的父状态不推断完成", parent("unknown"), None, [], DeployState.DISPATCHING, {}),
        ("无子任务即无需变更", parent("success"), None, [], DeployState.SUCCESS, {}),
        (
            "全部主机成功",
            parent("success", "wf-1"),
            [{"workflow_id": "wf-1", "status": "success", "bk_host_id": [1, 2]}],
            [operation(1, "success"), operation(2, "success")],
            DeployState.SUCCESS,
            {1: DeployState.SUCCESS, 2: DeployState.SUCCESS},
        ),
        (
            "任一主机仍在执行",
            parent("success", "wf-1"),
            [{"workflow_id": "wf-1", "status": "running", "bk_host_id": [1, 2]}],
            [operation(1, "success"), operation(2, "launched")],
            DeployState.RUNNING,
            {1: DeployState.SUCCESS, 2: DeployState.RUNNING},
        ),
        (
            "任一主机失败",
            parent("success", "wf-1"),
            [{"workflow_id": "wf-1", "status": "failed", "bk_host_id": [1, 2]}],
            [operation(1, "timeout", "inst-1"), operation(2, "success", "inst-2")],
            DeployState.FAILED,
            {1: DeployState.FAILED, 2: DeployState.SUCCESS},
        ),
        (
            "子任务运行中但主机操作尚未生成",
            parent("success", "wf-1"),
            [{"workflow_id": "wf-1", "status": "running", "bk_host_id": [1, 2]}],
            [operation(1, "success")],
            DeployState.RUNNING,
            {1: DeployState.SUCCESS, 2: DeployState.RUNNING},
        ),
        (
            "共享子任务里其他策略的主机不参与汇总",
            parent("success", "wf-1"),
            [{"workflow_id": "wf-1", "status": "failed", "bk_host_id": [1, 99]}],
            [operation(1, "success"), operation(99, "failed")],
            DeployState.SUCCESS,
            {1: DeployState.SUCCESS},
        ),
    ],
)
def test_execution_state(case, parent_resp, child_resp, operations, expected, expected_hosts):
    """父任务只表示派发；派发成功后按本次主机在子插件任务中的操作汇总。"""
    nodeman = FakeNodeMan(
        {
            "list_deploy_policy_workflows": parent_resp,
            "list_plugin_workflows": {"total": len(child_resp or []), "items": child_resp or []},
            "list_plugin_workflow_operations": {"total": len(operations), "operations": operations},
        }
    )

    state = SubConfigPolicies(nodeman).execution_state(TENANT, "wf-parent", [1, 2])

    assert state.state is expected, case
    assert {item.bk_host_id: item.state for item in state.hosts} == expected_hosts, case
    assert nodeman.params("list_deploy_policy_workflows")[0]["exact_include_conditions"] == {
        "workflow_id": ["wf-parent"]
    }
    for params in nodeman.params("list_plugin_workflow_operations"):
        # 只查询本次下发的主机
        assert params["exact_include_conditions"] == {"bk_host_id": [1, 2]}


def test_execution_state_keeps_failed_operation_instance():
    """失败主机带上最近一次操作实例 ID，用于查询失败日志。"""
    nodeman = FakeNodeMan(
        {
            "list_deploy_policy_workflows": parent("success", "wf-1"),
            "list_plugin_workflows": {"total": 1, "items": [{"workflow_id": "wf-1", "status": "failed"}]},
            "list_plugin_workflow_operations": {
                "total": 1,
                "operations": [operation(1, "failed", "inst-1")],
            },
        }
    )
    state = SubConfigPolicies(nodeman).execution_state(TENANT, "wf-parent", [1])
    assert state.hosts[0].oper_inst_id == "inst-1"


def test_failure_logs_prefers_error_level_of_failed_actions():
    """只取失败步骤的日志，优先错误级别；没有错误级别时保留该步骤的全部日志。"""
    nodeman = FakeNodeMan(
        {
            "get_plugin_workflow_operation_instance_log": {
                "oper_inst_logs": {
                    "act_fetch": {
                        "life_cycle": {"state": "success"},
                        "display_name_zh": "获取进程",
                        "message": {"logs": [{"level": "error", "text_zh": "不应出现"}]},
                    },
                    "act_render": {
                        "life_cycle": {"state": "failed"},
                        "display_name_zh": "渲染配置",
                        "message": {
                            "logs": [
                                {"level": "info", "text_zh": "开始渲染"},
                                {"level": "error", "text_zh": "模板渲染失败"},
                            ]
                        },
                    },
                    "act_push": {
                        "life_cycle": {"state": "timeout"},
                        "display_name_zh": "推送配置",
                        "message": {"logs": [{"level": "info", "text_en": "push timeout"}]},
                    },
                },
                "extra_execution_logs": {"logs": [{"level": "warning", "text_zh": "重试次数已用完"}]},
            }
        }
    )

    lines = SubConfigPolicies(nodeman).failure_logs(TENANT, "inst-1")

    assert lines == ["[渲染配置] 模板渲染失败", "[推送配置] push timeout", "重试次数已用完"]
    assert SubConfigPolicies(Mock()).failure_logs(TENANT, "") == []
