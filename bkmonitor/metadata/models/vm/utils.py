"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2021 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
"""

import json
import logging
import random

from django.conf import settings
from django.db.models import Q
from tenacity import RetryError, retry, stop_after_attempt, wait_exponential

from constants.data_source import DATA_LINK_V3_VERSION_NAME, DATA_LINK_V4_VERSION_NAME
from core.drf_resource import api
from core.prometheus import metrics
from metadata.models import (
    AccessVMRecord,
    BCSClusterInfo,
    BcsFederalClusterInfo,
    ClusterInfo,
    DataSource,
    DataSourceOption,
)
from metadata.models.data_link import DataLink
from metadata.models.data_link.constants import DataLinkResourceStatus
from metadata.models.data_link.service import create_vm_data_link
from metadata.models.data_link.utils import (
    compose_bkdata_data_id_name,
    compose_bkdata_table_id,
)
from metadata.models.space.constants import EtlConfigs
from metadata.models.vm.bk_data import BkDataAccessor, access_vm
from metadata.models.vm.config import BkDataStorageWithDataID
from metadata.models.vm.constants import (
    ACCESS_DATA_LINK_FAILURE_STATUS,
    ACCESS_DATA_LINK_SUCCESS_STATUS,
    BKDATA_NS_TIMESTAMP_DATA_ID_LIST,
    TimestampLen,
)

logger = logging.getLogger("metadata")


def refine_bkdata_kafka_info():
    from metadata.models import ClusterInfo

    """获取接入计算平台时，使用的 kafka 信息"""
    kafka_clusters = ClusterInfo.objects.filter(cluster_type=ClusterInfo.TYPE_KAFKA).values("cluster_id", "domain_name")
    kafka_domain_cluster_id = {obj["domain_name"]: obj["cluster_id"] for obj in kafka_clusters}
    # 通过集群平台获取可用的 kafka host
    bkdata_kafka_data = api.bkdata.get_kafka_info()[0]
    bkdata_kafka_host_list = bkdata_kafka_data.get("ip_list", "").split(",")

    # NOTE: 获取 metadata 和接口返回的交集，然后任取其中一个; 如果不存在，则直接报错
    existed_host_list = list(set(bkdata_kafka_host_list) & set(kafka_domain_cluster_id.keys()))
    if not existed_host_list:
        logger.error("bkdata kafka host not registered ClusterInfo, bkdata resp: %s", json.dumps(bkdata_kafka_data))
        raise ValueError("bkdata kafka host not registered ClusterInfo")

    # 返回数据
    host = random.choice(existed_host_list)
    cluster_id = kafka_domain_cluster_id[host]
    logger.info("refine exist kafka, cluster_id: %s, host: %s", cluster_id, host)
    return {"cluster_id": cluster_id, "host": host}


def access_bkdata(bk_biz_id: int, table_id: str, data_id: int):
    """根据类型接入计算平台

    1. 仅针对接入 influxdb 类型

    当出现异常时，记录日志，然后通过告警进行通知
    """
    logger.info("bk_biz_id: %s, table_id: %s, data_id: %s start access vm", bk_biz_id, table_id, data_id)

    from metadata.models import AccessVMRecord, KafkaStorage, Space, SpaceVMInfo

    # NOTE: 0 业务没有空间信息，不需要查询或者创建空间及空间关联的 vm
    space_data = {}
    try:
        # NOTE: 这里 bk_biz_id 为整型
        space_data = Space.objects.get_space_info_by_biz_id(int(bk_biz_id))
    except Exception as e:
        logger.error("get space error by biz_id: %s, error: %s", bk_biz_id, e)

    # 如果不在空间接入 vm 的记录中，则创建记录
    if (
        space_data
        and not SpaceVMInfo.objects.filter(
            space_type=space_data["space_type"], space_id=space_data["space_id"]
        ).exists()
    ):
        SpaceVMInfo.objects.create_record(space_type=space_data["space_type"], space_id=space_data["space_id"])

    # 检查是否已经写入 kafka storage，如果已经存在，认为已经接入 vm，则直接返回
    if AccessVMRecord.objects.filter(result_table_id=table_id).exists():
        logger.info("table_id: %s has already been created", table_id)
        return

    # 获取数据源类型、集群等信息
    data_type_cluster = get_data_type_cluster(data_id)
    data_type = data_type_cluster.get("data_type")

    # 获取 vm 集群名称
    vm_cluster = get_vm_cluster_id_name(space_data.get("space_type", ""), space_data.get("space_id", ""))
    vm_cluster_name = vm_cluster.get("cluster_name")
    # 调用接口接入数据平台
    bcs_cluster_id = data_type_cluster.get("bcs_cluster_id")
    data_name_and_topic = get_bkbase_data_name_and_topic(table_id)
    timestamp_len = get_timestamp_len(data_id)
    try:
        vm_data = access_vm_by_kafka(table_id, data_name_and_topic["data_name"], vm_cluster_name, timestamp_len)
        # 上报指标（接入成功）
        report_metadata_data_link_access_metric(
            version=DATA_LINK_V3_VERSION_NAME,
            data_id=data_id,
            biz_id=bk_biz_id,
            status=ACCESS_DATA_LINK_SUCCESS_STATUS,
            strategy=DataLink.BK_STANDARD_V2_TIME_SERIES,
        )
    except Exception as e:
        logger.error("access vm error, %s", e)
        # 上报指标（接入失败）
        report_metadata_data_link_access_metric(
            version=DATA_LINK_V3_VERSION_NAME,
            data_id=data_id,
            biz_id=bk_biz_id,
            status=ACCESS_DATA_LINK_FAILURE_STATUS,
            strategy=DataLink.BK_STANDARD_V2_TIME_SERIES,
        )
        return

    # 如果接入返回为空，则直接返回
    if vm_data.get("err_msg"):
        logger.error("access vm error")
        return

    # 创建 KafkaStorage 和 AccessVMRecord 记录
    try:
        if not vm_data.get("kafka_storage_exist"):
            KafkaStorage.create_table(
                table_id=table_id,
                is_sync_db=True,
                storage_cluster_id=vm_data["cluster_id"],
                topic=data_name_and_topic["topic_name"],
                use_default_format=False,
            )
    except Exception as e:
        logger.error("create KafkaStorage error for access vm: %s", e)

    try:
        AccessVMRecord.objects.create(
            data_type=data_type,
            result_table_id=table_id,
            bcs_cluster_id=bcs_cluster_id,
            storage_cluster_id=vm_data["cluster_id"],
            vm_cluster_id=vm_cluster["cluster_id"],
            bk_base_data_id=vm_data["bk_data_id"],  # 计算平台数据ID
            bk_base_data_name=data_name_and_topic["data_name"],  # 计算平台数据名称
            vm_result_table_id=vm_data["clean_rt_id"],
        )
    except Exception as e:
        logger.error("create AccessVMRecord error for access vm: %s", e)

    logger.info("bk_biz_id: %s, table_id: %s, data_id: %s access vm successfully", bk_biz_id, table_id, data_id)

    # NOTE: 针对 bcs 添加合流流程
    # 1. 当前环境允许合流操作
    # 2. 合流的目的rt存在
    # 3. 当出现异常时，记录对应日志
    if (
        settings.BCS_DATA_CONVERGENCE_CONFIG.get("is_enabled")
        and settings.BCS_DATA_CONVERGENCE_CONFIG.get("k8s_metric_rt")
        and settings.BCS_DATA_CONVERGENCE_CONFIG.get("custom_metric_rt")
        and bcs_cluster_id
    ):
        try:
            data_name_and_dp_id = get_bcs_convergence_data_name_and_dp_id(table_id)
            clean_data = BkDataAccessor(
                bk_table_id=data_name_and_dp_id["data_name"],
                data_hub_name=data_name_and_dp_id["data_name"],
                timestamp_len=timestamp_len,
            ).clean
            clean_data["result_table_id"] = (
                settings.BCS_DATA_CONVERGENCE_CONFIG["k8s_metric_rt"]
                if data_type == AccessVMRecord.BCS_CLUSTER_K8S
                else settings.BCS_DATA_CONVERGENCE_CONFIG["custom_metric_rt"]
            )
            clean_data["processing_id"] = data_name_and_dp_id["dp_id"]
            # 创建清洗
            api.bkdata.databus_cleans(**clean_data)
            # 启动
            api.bkdata.start_databus_cleans(
                result_table_id=clean_data["result_table_id"],
                storages=["kafka"],
                processing_id=data_name_and_dp_id["dp_id"],
            )
        except Exception as e:
            logger.error(
                "bcs convergence create or start data clean error, table_id: %s, params: %s, error: %s",
                table_id,
                json.dumps(clean_data),
                e,
            )


def access_vm_by_kafka(table_id: str, raw_data_name: str, vm_cluster_name: str, timestamp_len: int) -> dict:
    """通过 kafka 配置接入 vm"""
    from metadata.models import BkDataStorage, KafkaStorage, ResultTable

    kafka_storage_exist, storage_cluster_id = True, 0
    try:
        kafka_storage = KafkaStorage.objects.get(table_id=table_id)
        storage_cluster_id = kafka_storage.storage_cluster_id
    except Exception as e:
        logger.info("query kafka storage error %s", e)
        kafka_storage_exist = False

    # 如果不存在，则直接创建
    if not kafka_storage_exist:
        try:
            kafka_data = refine_bkdata_kafka_info()
        except Exception as e:
            logger.error("get bkdata kafka host error, table_id: %s, error: %s", table_id, e)
            return {"err_msg": f"request vm api error, {e}"}
        storage_cluster_id = kafka_data["cluster_id"]
        try:
            vm_data = access_vm(
                raw_data_name=raw_data_name,
                vm_cluster=vm_cluster_name,
                timestamp_len=timestamp_len,
            )
            vm_data["cluster_id"] = storage_cluster_id
            return vm_data
        except Exception as e:
            logger.error("request vm api error, table_id: %s, error: %s", table_id, e)
            return {"err_msg": f"request vm api error, {e}"}
    # 创建清洗和入库 vm
    bk_base_data = BkDataStorage.objects.filter(table_id=table_id).first()
    if not bk_base_data:
        bk_base_data = BkDataStorage.objects.create(table_id=table_id)
    if bk_base_data.raw_data_id == -1:
        result_table = ResultTable.objects.get(table_id=table_id)
        bk_base_data.create_databus_clean(result_table)
    # 重新读取一遍数据
    bk_base_data.refresh_from_db()
    raw_data_name = get_bkbase_data_name_and_topic(table_id)["data_name"]
    clean_data = BkDataAccessor(
        bk_table_id=raw_data_name, data_hub_name=raw_data_name, vm_cluster=vm_cluster_name, timestamp_len=timestamp_len
    ).clean
    clean_data.update(
        {
            "bk_biz_id": settings.DEFAULT_BKDATA_BIZ_ID,
            "raw_data_id": bk_base_data.raw_data_id,
            "clean_config_name": raw_data_name,
            "kafka_storage_exist": kafka_storage_exist,
        }
    )
    clean_data["json_config"] = json.dumps(clean_data["json_config"])
    try:
        bkbase_result_table_id = api.bkdata.databus_cleans(**clean_data)["result_table_id"]
        # 启动
        api.bkdata.start_databus_cleans(result_table_id=bkbase_result_table_id, storages=["kafka"])
    except Exception as e:
        logger.error(
            "create or start data clean error, table_id: %s, params: %s, error: %s", table_id, json.dumps(clean_data), e
        )
        return {"err_msg": f"request clean api error, {e}"}
    # 接入 vm
    try:
        storage_params = BkDataStorageWithDataID(bk_base_data.raw_data_id, raw_data_name, vm_cluster_name).value
        api.bkdata.create_data_storages(**storage_params)
        return {
            "clean_rt_id": f"{settings.DEFAULT_BKDATA_BIZ_ID}_{raw_data_name}",
            "bk_data_id": bk_base_data.raw_data_id,
            "cluster_id": storage_cluster_id,
            "kafka_storage_exist": kafka_storage_exist,
        }
    except Exception as e:
        logger.error("create vm storage error, %s", e)
        return {"err_msg": f"request vm storage api error, {e}"}


def get_data_type_cluster(data_id: int) -> dict:
    from metadata.models import AccessVMRecord, BCSClusterInfo

    # NOTE: data id 不允许跨集群
    bcs_cluster = BCSClusterInfo.objects.filter(Q(K8sMetricDataID=data_id) | Q(CustomMetricDataID=data_id)).first()
    # 获取对应的类型
    data_type = AccessVMRecord.ACCESS_VM
    bcs_cluster_id = None
    if not bcs_cluster:
        data_type = AccessVMRecord.USER_CUSTOM
    elif bcs_cluster.K8sMetricDataID == data_id:
        data_type = AccessVMRecord.BCS_CLUSTER_K8S
        bcs_cluster_id = bcs_cluster.cluster_id
    else:
        data_type = AccessVMRecord.BCS_CLUSTER_CUSTOM
        bcs_cluster_id = bcs_cluster.cluster_id
    return {"data_type": data_type, "bcs_cluster_id": bcs_cluster_id}


def report_metadata_data_link_access_metric(
    version: str,
    status: int,
    biz_id: int,
    data_id: int,
    strategy: str,
) -> None:
    """
    上报接入链路相关指标
    @param version: 链路版本（V3/V4）
    @param status: 接入状态（失败-1/成功1） 以是否成功向bkbase发起请求为准
    @param biz_id: 业务ID
    @param data_id: 数据ID
    @param strategy: 链路策略（套餐类型）
    """
    try:
        logger.info("try to report metadata data link component status metric,data_id->[%s]", data_id)
        metrics.METADATA_DATA_LINK_ACCESS_TOTAL.labels(
            version=version, biz_id=biz_id, strategy=strategy, status=status
        ).inc()
        metrics.report_all()
    except Exception as err:  # pylint: disable=broad-except
        logger.error("report metadata data link access metric error->[%s],data_id->[%s]", err, data_id)
        return


def report_metadata_data_link_status_info(data_link_name: str, biz_id: str, kind: str, status: str):
    """
    上报数据链路状态信息
    @param data_link_name: 数据链路名称
    @param biz_id: 业务ID
    @param kind: 数据链路类型
    @param status: 数据链路状态
    """
    try:
        logger.info("try to report metadata data link status info,data_link_name->[%s]", data_link_name)
        status_number = DataLinkResourceStatus.get_choice_value(status)
        metrics.METADATA_DATA_LINK_STATUS_INFO.labels(data_link_name=data_link_name, biz_id=biz_id, kind=kind).set(
            status_number
        )
    except Exception as err:
        logger.error("report metadata data link status info error->[%s],data_link_name->[%s]", err, data_link_name)


def get_vm_cluster_id_name(
    space_type: str | None = "", space_id: str | None = "", vm_cluster_name: str | None = ""
) -> dict:
    """获取 vm 集群 ID 和名称

    1. 如果 vm 集群名称存在，则需要查询到对应的ID，如果查询不到，则需要抛出异常
    2. 如果 vm 集群名称不存在，则需要查询空间是否已经接入过，如果已经接入过，则可以直接获取
    3. 如果没有接入过，则需要使用默认值
    """
    from metadata.models import ClusterInfo, SpaceVMInfo

    # vm 集群名称存在
    if vm_cluster_name:
        clusters = ClusterInfo.objects.filter(cluster_type=ClusterInfo.TYPE_VM, cluster_name=vm_cluster_name)
        if not clusters.exists():
            logger.error(
                "query vm cluster error, vm_cluster_name: %s not found, please register to clusterinfo", vm_cluster_name
            )
            raise ValueError(f"vm_cluster_name: {vm_cluster_name} not found")
        cluster = clusters.first()
        return {"cluster_id": cluster.cluster_id, "cluster_name": cluster.cluster_name}
    elif space_type and space_id:
        objs = SpaceVMInfo.objects.filter(space_type=space_type, space_id=space_id)
        if not objs.exists():
            logger.warning("space_type: %s, space_id: %s not access vm", space_type, space_id)
        else:
            try:
                cluster = ClusterInfo.objects.get(cluster_id=objs.first().vm_cluster_id)
            except Exception:
                logger.error(
                    "space_type: %s, space_id: %s, cluster_id: %s not found",
                    space_type,
                    space_id,
                    objs.first().vm_cluster_id,
                )
                raise ValueError(f"space_type: {space_type}, space_id: {space_id} not found vm cluster")
            return {"cluster_id": cluster.cluster_id, "cluster_name": cluster.cluster_name}

    # 获取默认 VM 集群
    clusters = ClusterInfo.objects.filter(cluster_type=ClusterInfo.TYPE_VM, is_default_cluster=True)
    if not clusters.exists():
        logger.error("not found vm default cluster")
        raise ValueError("not found vm default cluster")
    cluster = clusters.first()
    return {"cluster_id": cluster.cluster_id, "cluster_name": cluster.cluster_name}


def get_storage_cluster_id_name_for_space(
    storage_type=ClusterInfo.TYPE_VM,
    space_type: str | None = "",
    space_id: str | None = "",
    cluster_name: str | None = "",
) -> dict:
    """
    TODO 待数据与SpaceVMInfo打平后,将原先选择逻辑切换至SpaceRelatedStorageInfo
    获取指定空间关联的指定存储类型的集群ID和名称
    @param storage_type: 存储类型,默认VM
    @param space_type: 空间类型
    @param space_id: 空间ID
    @param cluster_name: 集群名称
    @return: {集群ID,集群名称}
    1. 如果传递了指定集群名称,则查询是否存在指定名称的集群,返回其ID和名称
    2. 如果传递了空间类型和空间ID,查询该空间是否有配置的指定存储集群记录,如有记录,则返回记录的集群ID和名称
    3. 如果传递了空间类型和空间ID,查询该空间是否有配置的指定存储集群记录,如没有记录,则返回默认集群ID和名称
    4. 如果没有传递空间类型和空间ID,则返回默认集群ID和名称
    """
    from metadata.models import ClusterInfo, SpaceRelatedStorageInfo

    if cluster_name:  # 指定了集群名称,查询并返回其信息
        clusters = ClusterInfo.objects.filter(cluster_type=storage_type, cluster_name=cluster_name)
        if not clusters.exists():
            logger.error(
                "get_storage_cluster_id_name_for_space:query cluster error, cluster_name->%s not found, "
                "please register to clusterinfo,storage_type->[%s]",
                cluster_name,
                storage_type,
            )
            raise ValueError(f"cluster_name: {cluster_name} not found")
        cluster = clusters.last()
    elif space_type and space_id:  # 指定了空间,查询空间关联记录 / 创建记录
        space_related_storage_records = SpaceRelatedStorageInfo.objects.filter(
            space_type_id=space_type, space_id=space_id, storage_type=storage_type
        )
        if not space_related_storage_records.exists():  # 如果没有关联记录,使用默认集群并创建记录
            logger.info(
                "get_storage_cluster_id_name_for_space:space_type->[%s], space_id->[%s] does not have "
                "storage_record,use default,and will create record later",
                space_type,
                space_id,
            )
            cluster = ClusterInfo.objects.filter(cluster_type=storage_type, is_default_cluster=True).last()

            # 使用默认集群,创建关联记录
            SpaceRelatedStorageInfo.create_space_related_storage_record(
                space_type_id=space_type,
                space_id=space_id,
                storage_type=storage_type,
                cluster_id=cluster.cluster_id,
            )
        else:  # 如果有关联记录,则直接返回记录的集群ID和名称
            cluster = ClusterInfo.objects.get(cluster_id=space_related_storage_records.last().cluster_id)
    else:
        # 没有传递任何参数,则返回默认集群ID和名称
        cluster = ClusterInfo.objects.filter(cluster_type=storage_type, is_default_cluster=True).last()

    return {"cluster_id": cluster.cluster_id, "cluster_name": cluster.cluster_name}


def get_bkbase_data_name_and_topic(table_id: str) -> dict:
    """获取 bkbase 的结果表名称"""
    # 如果以 '__default__'结尾，则取前半部分
    if table_id.endswith("__default__"):
        table_id = table_id.split(".__default__")[0]
    name = f"{table_id.replace('-', '_').replace('.', '_').replace('__', '_')[-40:]}"
    # NOTE: 清洗结果表不能出现双下划线
    vm_name = f"vm_{name}".replace("__", "_")
    # 兼容部分场景中划线和下划线允许同时存在的情况
    is_exist = AccessVMRecord.objects.filter(vm_result_table_id__contains=vm_name).exists()
    if is_exist:
        if len(vm_name) > 45:
            vm_name = vm_name[:45]
        vm_name = vm_name + "_add"

    return {"data_name": vm_name, "topic_name": f"{vm_name}{settings.DEFAULT_BKDATA_BIZ_ID}"}


def get_bcs_convergence_data_name_and_dp_id(table_id: str) -> dict:
    """获取 bcs 合流对应的结果表及数据处理 ID"""
    if table_id.endswith("__default__"):
        table_id = table_id.split(".__default__")[0]
    name = f"{table_id.replace('-', '_').replace('.', '_').replace('__', '_')[-40:]}"
    # NOTE: 清洗结果表不能出现双下划线
    return {"data_name": f"dp_{name}", "dp_id": f"{settings.DEFAULT_BKDATA_BIZ_ID}_{name}_dp_metric_all"}


def get_timestamp_len(data_id: int | None = None, etl_config: str | None = None) -> int:
    """通过 data id 或者 etl config 获取接入 vm 是清洗时间的长度

    1. 如果 data id 在指定的白名单中，则为 纳米
    2. 其它，则为 毫秒
    """
    logger.info("get_timestamp_len: data_id: %s, etl_config: %s", data_id, etl_config)
    if data_id and data_id in BKDATA_NS_TIMESTAMP_DATA_ID_LIST:
        return TimestampLen.NANOSECOND_LEN.value

    # Note: BCS集群接入场景时，由于事务中嵌套异步任务，可能导致数据未就绪
    # 新接入场景默认使用毫秒作为单位，若过程出现失败，直接返回默认单位，不应影响后续流程
    try:
        ds = DataSource.objects.get(bk_data_id=data_id)
        ds_option = DataSourceOption.objects.filter(bk_data_id=data_id, name=DataSourceOption.OPTION_ALIGN_TIME_UNIT)
        # 若存在对应配置项，优先使用配置的时间格式
        if ds_option.exists():
            logger.info("get_timestamp_len: ds_option exists,ds_option ALIGN_TIME)UNIT: %s", ds_option.first().value)
            return TimestampLen.get_len_choices(ds_option.first().value)
        # Note： 历史原因，针对脚本等配置，若不存在对应时间戳配置，默认单位应为秒
        if ds.etl_config in {EtlConfigs.BK_EXPORTER.value, EtlConfigs.BK_STANDARD.value}:
            logger.info("get_timestamp_len: ds.etl_config: %s,will use second as time format", ds.etl_config)
            return TimestampLen.SECOND_LEN.value
    except Exception as e:
        logger.error("get_timestamp_len:failed %s", e)
    return TimestampLen.MILLISECOND_LEN.value


@retry(stop=stop_after_attempt(4), wait=wait_exponential(multiplier=1, min=1, max=10))
def get_data_source(data_id):
    """
    根据 data_id 获取对应的 DataSource，重试三次，间隔1->2-4秒，规避事务未及时提交导致的查询失败问题
    """
    return DataSource.objects.get(bk_data_id=data_id)


def access_v2_bkdata_vm(bk_biz_id: int, table_id: str, data_id: int):
    """
    接入计算平台V4链路
    @param bk_biz_id: 业务ID
    @param table_id: 结果表ID
    @param data_id: 数据源ID
    """
    logger.info("bk_biz_id: %s, table_id: %s, data_id: %s start access v2 vm", bk_biz_id, table_id, data_id)

    from metadata.models import AccessVMRecord, DataSource, Space, SpaceVMInfo

    # 0. 确认空间信息
    # NOTE: 0 业务没有空间信息，不需要查询或者创建空间及空间关联的 vm
    space_data = {}
    try:
        # NOTE: 这里 bk_biz_id 为整型
        space_data = Space.objects.get_space_info_by_biz_id(int(bk_biz_id))
    except Exception as e:  # pylint: disable=broad-except
        logger.error("get space error by biz_id: %s, error: %s", bk_biz_id, e)

    # 1，获取VM集群信息
    vm_cluster = get_vm_cluster_id_name(
        space_type=space_data.get("space_type", ""), space_id=space_data.get("space_id", "")
    )
    # 1.1 校验是否存在SpaceVMInfo记录，如果不存在，则进行创建记录
    if (
        space_data
        and not SpaceVMInfo.objects.filter(
            space_type=space_data["space_type"], space_id=space_data["space_id"]
        ).exists()
    ):
        SpaceVMInfo.objects.create_record(
            space_type=space_data["space_type"], space_id=space_data["space_id"], vm_cluster_id=vm_cluster["cluster_id"]
        )

    try:
        # 1.2 NOTE: 这里可能因为事务+异步的原因，导致查询时DB中的DataSource未就绪，添加重试机制
        ds = get_data_source(data_id)
    except RetryError as e:
        logger.error("create vm data link error, get data_id: %s, error: %s", data_id, e.__cause__)
        return
    except DataSource.DoesNotExist:
        logger.error("create vm data link error, data_id: %s not found", data_id)
        return

    # 2. 获取 vm 集群名称
    vm_cluster_name = vm_cluster.get("cluster_name")

    # 3. 获取数据源对应的集群 ID
    data_type_cluster = get_data_type_cluster(data_id=data_id)
    # 4. 检查是否已经接入过VM，若已经接入过VM，尝试进行联邦集群检查和创建联邦汇聚链路操作
    if AccessVMRecord.objects.filter(result_table_id=table_id).exists():
        logger.info("table_id: %s has already been created,now try to create fed vm data link", table_id)

        create_fed_bkbase_data_link(
            monitor_table_id=table_id,
            data_source=ds,
            storage_cluster_name=vm_cluster_name,
            bcs_cluster_id=data_type_cluster["bcs_cluster_id"],
        )
        return

    # 5. 接入 vm 链路
    try:
        # 如果开启了新版接入方式，使用新版方式接入，灰度验证中
        if settings.ENABLE_V2_ACCESS_BKBASE_METHOD:
            logger.info(
                "access_v2_bkdata_vm: enable_v2_access_bkbase_method is True, now try to create bkbase data link"
            )
            bcs_cluster_id = None
            bcs_record = BCSClusterInfo.objects.filter(K8sMetricDataID=ds.bk_data_id)
            if bcs_record:
                bcs_cluster_id = bcs_record.first().cluster_id

            create_bkbase_data_link(
                data_source=ds,
                monitor_table_id=table_id,
                storage_cluster_name=vm_cluster_name,
                bcs_cluster_id=bcs_cluster_id,
            )
        else:
            logger.info("access_v2_bkdata_vm: enable_v2_access_bkbase_method is False")
            create_vm_data_link(
                table_id=table_id,
                data_source=ds,
                vm_cluster_name=vm_cluster_name,
                bcs_cluster_id=data_type_cluster["bcs_cluster_id"],
            )

        report_metadata_data_link_access_metric(
            version=DATA_LINK_V4_VERSION_NAME,
            data_id=data_id,
            biz_id=bk_biz_id,
            status=ACCESS_DATA_LINK_SUCCESS_STATUS,
            strategy=DataLink.BK_STANDARD_V2_TIME_SERIES,
        )
    except RetryError as e:
        logger.error("create vm data link error, table_id: %s, data_id: %s, error: %s", table_id, data_id, e.__cause__)
        report_metadata_data_link_access_metric(
            version=DATA_LINK_V4_VERSION_NAME,
            data_id=data_id,
            biz_id=bk_biz_id,
            status=ACCESS_DATA_LINK_FAILURE_STATUS,
            strategy=DataLink.BCS_FEDERAL_SUBSET_TIME_SERIES,
        )
        return
    except Exception as e:  # pylint: disable=broad-except
        logger.error("create vm data link error, table_id: %s, data_id: %s, error: %s", table_id, data_id, e)
        report_metadata_data_link_access_metric(
            version=DATA_LINK_V4_VERSION_NAME,
            data_id=data_id,
            biz_id=bk_biz_id,
            status=ACCESS_DATA_LINK_FAILURE_STATUS,
            strategy=DataLink.BCS_FEDERAL_SUBSET_TIME_SERIES,
        )
        return

    try:
        # 创建联邦
        create_fed_bkbase_data_link(
            monitor_table_id=table_id,
            data_source=ds,
            storage_cluster_name=vm_cluster_name,
            bcs_cluster_id=data_type_cluster["bcs_cluster_id"],
        )
        report_metadata_data_link_access_metric(
            version=DATA_LINK_V4_VERSION_NAME,
            data_id=data_id,
            biz_id=bk_biz_id,
            status=ACCESS_DATA_LINK_SUCCESS_STATUS,
            strategy=DataLink.BCS_FEDERAL_SUBSET_TIME_SERIES,
        )
    except Exception as e:  # pylint: disable=broad-except
        logger.error("create fed vm data link error, table_id: %s, data_id: %s, error: %s", table_id, data_id, e)
        report_metadata_data_link_access_metric(
            version=DATA_LINK_V4_VERSION_NAME,
            data_id=data_id,
            biz_id=bk_biz_id,
            status=ACCESS_DATA_LINK_FAILURE_STATUS,
            strategy=DataLink.BCS_FEDERAL_SUBSET_TIME_SERIES,
        )
        return


def create_bkbase_data_link(
    data_source: DataSource,
    monitor_table_id: str,
    storage_cluster_name: str,
    data_link_strategy: str = DataLink.BK_STANDARD_V2_TIME_SERIES,
    namespace: str | None = settings.DEFAULT_VM_DATA_LINK_NAMESPACE,
    bcs_cluster_id: str | None = None,
):
    """
    申请计算平台链路
    @param data_source: 数据源
    @param monitor_table_id: 监控平台自身结果表ID
    @param storage_cluster_name: 存储集群名称
    @param data_link_strategy: 链路策略
    @param namespace: 命名空间
    @param bcs_cluster_id: BCS集群ID
    """
    logger.info(
        "create_bkbase_data_link:try to access bkbase,data_id->[%s],storage_cluster_name->[%s],data_link_strategy->["
        "%s],namespace->[%s]",
        data_source.bk_data_id,
        storage_cluster_name,
        data_link_strategy,
        namespace,
    )
    # 0. 组装生成计算平台侧需要的data_name和rt_name
    bkbase_data_name = compose_bkdata_data_id_name(data_name=data_source.data_name)
    bkbase_rt_name = compose_bkdata_table_id(table_id=monitor_table_id)
    logger.info(
        "create_bkbase_data_link:try to access bkbase , data_id->[%s],bkbase_data_name->[%s],bkbase_vmrt_name->[%s]",
        data_source.bk_data_id,
        bkbase_data_name,
        bkbase_rt_name,
    )

    # 1. 判断是否是联邦代理集群链路
    if BcsFederalClusterInfo.objects.filter(fed_cluster_id=bcs_cluster_id, is_deleted=False).exists():
        logger.info("create_bkbase_data_link: bcs_cluster_id->[%s] is a federal proxy cluster!", bcs_cluster_id)
        data_link_strategy = DataLink.BCS_FEDERAL_PROXY_TIME_SERIES

    # TODO: 优化为MAP形式选取
    if data_source.etl_config == EtlConfigs.BK_EXPORTER.value:
        data_link_strategy = DataLink.BK_EXPORTER_TIME_SERIES
    elif data_source.etl_config == EtlConfigs.BK_STANDARD.value:
        data_link_strategy = DataLink.BK_STANDARD_TIME_SERIES

    # 2. 创建链路资源对象
    data_link_ins, _ = DataLink.objects.get_or_create(
        data_link_name=bkbase_data_name,
        namespace=namespace,
        data_link_strategy=data_link_strategy,
    )
    try:
        # 2. 尝试根据套餐，申请创建链路
        logger.info(
            "create_bkbase_data_link:try to access bkbase,data_id->[%s],storage_cluster_name->[%s],"
            "data_link_strategy->[%s],"
            "namespace->[%s]，monitor_table_id->[%s]",
            data_source.bk_data_id,
            storage_cluster_name,
            data_link_strategy,
            namespace,
            monitor_table_id,
        )
        data_link_ins.apply_data_link(
            data_source=data_source, table_id=monitor_table_id, storage_cluster_name=storage_cluster_name
        )
        # 2.1 上报链路接入指标
    except Exception as e:  # pylint: disable=broad-except
        logger.error(
            "create_bkbase_data_link: access bkbase error, data_id->[%s],storage_cluster_name->[%s],"
            "data_link_strategy->["
            "%s],namespace->[%s],error->[%s]",
            data_source.bk_data_id,
            storage_cluster_name,
            data_link_strategy,
            namespace,
            e,
        )
        raise e

    logger.info(
        "create_bkbase_data_link:try to sync metadata,data_id->[%s],storage_cluster_name->[%s],data_link_strategy->["
        "%s],"
        "namespace->[%s]，monitor_table_id->[%s]",
        data_source.bk_data_id,
        storage_cluster_name,
        data_link_strategy,
        namespace,
        monitor_table_id,
    )
    # 3. 同步更新元数据
    data_link_ins.sync_metadata(
        data_source=data_source,
        table_id=monitor_table_id,
        storage_cluster_name=storage_cluster_name,
    )

    # TODO：路由双写至旧的AccessVMRecord，完成灰度验证后，统一迁移至新表后删除
    storage_cluster_id = ClusterInfo.objects.get(cluster_name=storage_cluster_name).cluster_id
    logger.info(
        "create_bkbase_data_link:try to write AccessVMRecord,data_id->[%s],storage_cluster_id->[%s],"
        "data_link_strategy->[%s]",
        data_source.bk_data_id,
        storage_cluster_id,
        data_link_strategy,
    )
    AccessVMRecord.objects.update_or_create(
        result_table_id=monitor_table_id,
        bk_base_data_id=data_source.bk_data_id,
        bk_base_data_name=bkbase_data_name,
        defaults={
            "vm_cluster_id": storage_cluster_id,
            "vm_result_table_id": f"{settings.DEFAULT_BKDATA_BIZ_ID}_{bkbase_rt_name}",
            "bcs_cluster_id": bcs_cluster_id,
        },
    )
    logger.info(
        "create_bkbase_data_link:access bkbase success,data_id->[%s],storage_cluster_name->[%s],data_link_strategy->["
        "%s]",
        data_source.bk_data_id,
        storage_cluster_name,
        data_link_strategy,
    )


def create_fed_bkbase_data_link(
    monitor_table_id: str,
    data_source: DataSource,
    storage_cluster_name: str,
    bcs_cluster_id: str,
    namespace: str | None = settings.DEFAULT_VM_DATA_LINK_NAMESPACE,
):
    """
    创建联邦集群汇聚链路（子集群->代理集群）
    """
    from metadata.models import BcsFederalClusterInfo
    from metadata.models.data_link.utils import is_k8s_metric_data_id

    logger.info(
        "create_fed_bkbase_data_link: bcs_cluster_id->[%s],data_id->[%s] start to create fed_bkbase_data_link",
        bcs_cluster_id,
        data_source.bk_data_id,
    )
    federal_records = BcsFederalClusterInfo.objects.filter(sub_cluster_id=bcs_cluster_id, is_deleted=False)

    # 若不存在对应联邦集群记录 / 非K8S内建指标数据，直接返回
    if not (federal_records.exists() and is_k8s_metric_data_id(data_name=data_source.data_name)):
        logger.info(
            "create_fed_bkbase_data_link: bcs_cluster_id->[%s],data_id->[%s] does not belong to any federal "
            "topo,return",
            bcs_cluster_id,
            data_source.bk_data_id,
        )
        return

    bkbase_data_name = compose_bkdata_data_id_name(
        data_name=data_source.data_name, strategy=DataLink.BCS_FEDERAL_SUBSET_TIME_SERIES
    )
    # bkbase_rt_name = compose_bkdata_table_id(table_id=monitor_table_id)

    logger.info(
        "create_fed_bkbase_data_link: bcs_cluster_id->[%s],data_id->[%s],data_link_name->[%s] try to create "
        "fed_bkbase_data_link",
        bcs_cluster_id,
        data_source.bk_data_id,
        bkbase_data_name,
    )
    data_link_ins, _ = DataLink.objects.get_or_create(
        data_link_name=bkbase_data_name,
        namespace=namespace,
        data_link_strategy=DataLink.BCS_FEDERAL_SUBSET_TIME_SERIES,
    )

    try:
        logger.info(
            "create_fed_bkbase_data_link: bcs_cluster_id->[%s],data_id->[%s],table_id->[%s],data_link_name->[%s] try "
            "to access bkdata",
            bcs_cluster_id,
            data_source.bk_data_id,
            monitor_table_id,
            bkbase_data_name,
        )
        data_link_ins.apply_data_link(
            data_source=data_source,
            table_id=monitor_table_id,
            storage_cluster_name=storage_cluster_name,
            bcs_cluster_id=bcs_cluster_id,
        )
    except Exception as e:  # pylint: disable=broad-except
        logger.error(
            "create_bkbase_data_link: access bkbase error, data_id->[%s],data_link_name->[%s],bcs_cluster_id->[%s],"
            "storage_cluster_name->[%s],namespace->[%s],error->[%s]",
            data_source.bk_data_id,
            bkbase_data_name,
            bcs_cluster_id,
            storage_cluster_name,
            namespace,
            e,
        )
        raise e

    data_link_ins.sync_metadata(
        data_source=data_source,
        table_id=monitor_table_id,
        storage_cluster_name=storage_cluster_name,
    )
    logger.info(
        "create_fed_bkbase_data_link: data_link_name->[%s],data_id->[%s],bcs_cluster_id->[%s],storage_cluster_name->["
        "%s] create fed datalink successfully",
        bkbase_data_name,
        data_source.bk_data_id,
        bcs_cluster_id,
        storage_cluster_name,
    )


def check_create_fed_vm_data_link(cluster):
    """
    检查该集群是否需要以及是否完成联邦集群子集群的汇聚链路创建
    """
    from metadata.models import DataLinkResource, DataSource, DataSourceResultTable

    # 检查是否存在对应的联邦集群记录
    objs = BcsFederalClusterInfo.objects.filter(sub_cluster_id=cluster.cluster_id, is_deleted=False)
    # 若该集群为联邦集群的子集群且此前未创建联邦集群的汇聚链路，尝试创建
    if objs and not DataLinkResource.objects.filter(data_bus_name__contains=f"{cluster.K8sMetricDataID}_fed").exists():
        logger.info(
            f"check_create_fed_vm_data_link:cluster_id->{cluster.cluster_id} is federal sub cluster and has not create fed data "
            "link,try to create"
        )
        # 创建联邦汇聚链路
        try:
            ds = DataSource.objects.get(bk_data_id=cluster.K8sMetricDataID)
            table_id = DataSourceResultTable.objects.get(bk_data_id=cluster.K8sMetricDataID).table_id
            vm_cluster = get_vm_cluster_id_name(space_type="bkcc", space_id=str(cluster.bk_biz_id))
            create_fed_bkbase_data_link(
                monitor_table_id=table_id,
                data_source=ds,
                storage_cluster_name=vm_cluster.get("cluster_name"),
                bcs_cluster_id=cluster.cluster_id,
            )
            logger.info(f"check_create_fed_vm_data_link:success cluster_id->{cluster.cluster_id}")
        except Exception as e:  # pylint: disable=broad-except
            logger.error(f"check_create_fed_vm_data_link:error occurs cluster_id->{cluster.cluster_id},error->{str(e)}")
