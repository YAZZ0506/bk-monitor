"""
Tencent is pleased to support the open source community by making 蓝鲸智云 - 监控平台 (BlueKing - Monitor) available.
Copyright (C) 2017-2021 THL A29 Limited, a Tencent company. All rights reserved.
Licensed under the MIT License (the "License"); you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://opensource.org/licenses/MIT
Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
specific language governing permissions and limitations under the License.
"""

import concurrent.futures
import copy
import functools
import inspect
import json
import logging
import time
from collections import Counter

from django.conf import settings
from django.template import Context, Template
from django.utils.translation import gettext as _

from alarm_backends.core.cache import key
from alarm_backends.service.access.data.records import DataRecord
from alarm_backends.service.detect import AnomalyDataPoint, DataPoint
from alarm_backends.templatetags.unit import unit_auto_convert, unit_convert_min
from constants.aiops import SDKDetectStatus
from constants.strategy import OS_RESTART_METRIC_ID
from core.errors.alarm_backends.detect import (
    HistoryDataNotExists,
    InvalidAlgorithmsConfig,
    InvalidDataPoint,
)
from core.prometheus import metrics
from core.unit import load_unit

logger = logging.getLogger("detect")


class DetectContext(dict):
    def __getattr__(self, item):
        return self.__getitem__(item)


class Algorithms:
    """
    检测算法基类，定义一个算法对象。
    """

    desc_tpl = ""

    def __init__(self):
        self.expr = self.gen_expr()
        self.byte_code = compile(self.expr, "<string>", "eval")

    def extra_context(self, context):
        """
        To be implemented
        作用： 填充 变量或函数供表达式使用
        """
        return {}

    def gen_expr(self):
        """
        To be implemented
        作用：基于策略定义生成表达式
        """
        return "None"

    def get_context(self, data_point):
        """
        基于data_point提供上下文
        """
        # default context env
        context = DetectContext(data_point=data_point)
        context.update({prop: getattr(data_point, prop) for prop in DataPoint.context_field})

        context.update(self.extra_context(context))
        return context

    def _detect(self, data_point):
        # validate data_point fabric
        for attr in DataPoint.context_field:
            if not hasattr(data_point, attr):
                raise InvalidDataPoint(data_point=data_point)

        context = self.get_context(data_point)
        if not hasattr(data_point, "__debug__"):
            return eval(self.byte_code, {}, context)
        # debug only
        ret = False
        try:
            ret = eval(self.byte_code, {}, context)
            return ret
        except Exception as e:
            logger.exception(f"detect raise error: {e}")
            raise e
        finally:
            dirty_key = ["unit_auto_convert", "unit_convert_min", "item"]
            context_debug = "\n".join([f"\t{k}: {v}" for k, v in context.items() if k not in dirty_key])
            logger.info(
                f"detect datapoint with \nexpr: \t{self.expr}\ncontext:\n{context_debug}\n[detect result]: \t{ret}\n"
            )

    def detect(self, data_point):
        """
        返回异常数据点对象
        """
        if self._detect(data_point):
            anomaly_point = AnomalyDataPoint(data_point=data_point, detector=self)
            try:
                anomaly_point.anomaly_message = self._format_message(data_point)
            except Exception as e:
                logger.error(f"format anomaly message error: {e}")
                anomaly_point.anomaly_message = ""
            return [anomaly_point]

    def _format_message(self, data_point):
        """
        渲染异常描述
        """
        if not self.desc_tpl:
            return ""
        context = Context(self.get_context(data_point))
        return Template(self.desc_tpl).render(context)

    def detect_records(self, data_points, level):
        """
        detect service entry
        """
        if isinstance(data_points, DataPoint):
            data_points = [data_points]
        anomaly_points = []
        for data_point in data_points:
            try:
                check_result = self.detect(data_point)
            except Exception:
                continue
            if check_result:
                ap = self.gen_anomaly_point(data_point, check_result, level)
                logger.info(
                    f"[detect] strategy({ap.data_point.item.strategy.id}) item({ap.data_point.item.id}) level[{level}] 发现异常点: {ap.__dict__}"
                )
                anomaly_points.append(ap)

        return anomaly_points

    def anomaly_message_template_tuple(self, data_point):
        """
        异常描述模板，
        :param data_point:
        :return: 前缀和后缀 -> tuple
        """
        prefix = data_point.item.name
        unit = load_unit(data_point.unit)
        value, suffix = unit.fn.auto_convert(data_point.value, decimal=settings.POINT_PRECISION)
        suffix = _(", 当前值{value}{unit}").format(value=value, unit=suffix)
        return prefix, suffix

    def gen_anomaly_point(self, data_point, detect_result, level, auto_format=True):
        """
        :param data_point: 待检测点 -> DataPoint
        :param detect_result: 检测结果 -> list
        :param level: 告警级别 -> int
        :param auto_format: 自动拼接前后缀
        :return:
        """
        anomaly_message_prefix, anomaly_message_suffix = self.anomaly_message_template_tuple(data_point)

        if len(detect_result) == 1:
            ap = detect_result[0]
            if auto_format:
                ap.anomaly_message = anomaly_message_prefix + ap.anomaly_message + anomaly_message_suffix
        else:
            # 总结基于多算法检测出的异常点，生成新的异常点
            ap = AnomalyDataPoint(data_point, self)
            desc_list = []
            for child_ap in detect_result:
                ap.child_detector.append(child_ap.detector)
                desc_list.append(child_ap.anomaly_message)

            if auto_format:
                ap.anomaly_message = anomaly_message_prefix + _("且").join(desc_list) + anomaly_message_suffix
            else:
                ap.anomaly_message = _("且").join(desc_list)

        ap.anomaly_id = self._gen_anomaly_id(data_point, level)

        return ap

    @staticmethod
    def _gen_anomaly_id(data_point, level):
        """
        生成异常事件id
        "{dimensions_md5}.{timestamp}.{strategy_id}.{item_id}.{level}"
        其中dimensions_md5 和 timestamp 是data_point的record_id
        """
        return f"{data_point.record_id}.{data_point.item.strategy.id}.{data_point.item.id}.{level}"


class ExprDetectAlgorithms(Algorithms):
    """
    表达式算法, 算法检测的基础单元
    """

    def __init__(self, expr, desc_tpl=""):
        self.expr = expr
        self.desc_tpl = desc_tpl
        super().__init__()

    def gen_expr(self):
        return self.expr


class BasicAlgorithmsCollection(Algorithms):
    """
    内置算法集
    """

    config_serializer = None
    desc_tpl = ""
    # op is Or or And
    expr_op = "and"

    def __init__(self, config, unit=""):
        self.config = config or dict()
        self.validated_config = None
        self.validate_config(config)
        self.unit = unit
        self.detectors = list(self.gen_expr())
        for detector in self.detectors:
            detector.extra_context = self.extra_context
            detector.get_context = self.get_context
            detector.unit = self.unit

    def validate_config(self, config):
        """
        校验算法配置是否合法
        """
        self.validated_config = config
        if self.config_serializer is None:
            return

        if inspect.isclass(self.config_serializer):
            init_kwargs = {"data": config}
            # 不允许空列表的配置
            if hasattr(self.config_serializer, "child"):
                init_kwargs["allow_empty"] = False

            self.config_serializer = self.config_serializer(**init_kwargs)

        if not self.config_serializer.is_valid():
            raise InvalidAlgorithmsConfig(config=config)

        self.validated_config = self.config_serializer.validated_data

    def gen_expr(self):
        """
        重写该方法，返回一个表达式算法对象生成器
        """
        yield ExprDetectAlgorithms("None", self.desc_tpl)

    def detect(self, data_point):
        # 调用表达式检测算法的detect
        anomaly = []
        for detector in self.detectors:
            result = detector.detect(data_point)
            if not result:
                if self.expr_op == "and":
                    return []
            else:
                if self.expr_op == "or":
                    return result
                anomaly.extend(result)

        return anomaly

    def get_context(self, data_point):
        context = super().get_context(data_point)
        context.update(
            {"algorithm_unit": self.unit, "unit_auto_convert": unit_auto_convert, "unit_convert_min": unit_convert_min}
        )
        return context


class HistoryPointFetcher:
    def set_default(self, value: int):
        self._default = value

    def query_history_points(self, data_points):
        item = data_points[0].item
        # os_restart 策略优化
        if item.query_configs[0]["metric_id"] == OS_RESTART_METRIC_ID:
            item.query.expression = "a"
        # 按时间从小到大排序
        sorted_data_points = sorted(data_points, key=lambda x: x.timestamp)
        offsets = self.get_history_offsets(item)
        for offset in offsets:
            # offsets 支持区间（相邻offset之间差值等于interval的整数倍）批量查询
            if isinstance(offset, tuple):
                start, end = offset
            else:
                start = end = offset

            if end == 0:
                self._publish_history_points(item, data_points)
                continue

            records = []
            from_timestamp, until_timestamp = (
                sorted_data_points[0].timestamp - end,
                sorted_data_points[-1].timestamp - start + item.query_configs[0]["agg_interval"],
            )

            accessed = None
            for history_timestamp in range(from_timestamp, until_timestamp, item.query_configs[0]["agg_interval"]):
                if accessed is None:
                    accessed = True
                accessed = accessed and self._check_history_points(item, history_timestamp)

            if accessed:
                # 历史时刻的数据都已经查过
                continue

            item_records = item.query_record(from_timestamp, until_timestamp)
            for record in item_records:
                point = DataRecord(item, record)
                if point.value:
                    records.append(adapter_data_access_2_detect(point, item))

            self._local_history_storage = {}
            self._publish_history_points(item, records)

    def _check_history_points(self, item, history_timestamp):
        """
        检查历史时刻的数据是否已经拉取过，如果存在，则更新过期时间。
        """
        client = key.HISTORY_DATA_KEY.client
        history_key = key.HISTORY_DATA_KEY.get_key(
            strategy_id=item.strategy.id, item_id=item.id, timestamp=history_timestamp
        )
        return client.exists(history_key)

    def _publish_history_points(self, item, history_points):
        """
        发布历史时刻的数据
        """
        if not history_points:
            return
        pipeline = key.HISTORY_DATA_KEY.client.pipeline(transaction=False)
        history_key_maker = functools.partial(
            key.HISTORY_DATA_KEY.get_key, strategy_id=item.strategy.id, item_id=item.id
        )
        # bulk cache json data
        history_points_map = {}
        for point in history_points:
            points_with_timestamp_map = history_points_map.setdefault(point.timestamp, {})
            points_with_timestamp_map[point.record_id.split(".")[0]] = json.dumps(point.as_dict())

        for timestamp, _points_with_timestamp_map in history_points_map.items():
            history_key = history_key_maker(timestamp=timestamp)
            pipeline.hmset(history_key, _points_with_timestamp_map)
            pipeline.expire(history_key, key.HISTORY_DATA_KEY.ttl)
        pipeline.execute()

    def fetch_history_point(self, item, point, history_timestamp):
        """
        获取当前数据点对应的历史数据点
        """
        client = key.HISTORY_DATA_KEY.client
        history_key = key.HISTORY_DATA_KEY.get_key(
            strategy_id=item.strategy.id, item_id=item.id, timestamp=history_timestamp
        )
        if not getattr(self, "_local_history_storage", None):
            self._local_history_storage = {}

        if history_key not in self._local_history_storage:
            self._local_history_storage[history_key] = client.hgetall(history_key)

        raw_data = self._local_history_storage[history_key].get(point.record_id.split(".")[0])
        if not raw_data:
            if getattr(self, "_default", None) is not None:
                return DataPoint({"value": self._default, "time": history_timestamp}, item)
            return

        return DataPoint(json.loads(raw_data), item)

    def get_history_offsets(self, item):
        """
        获取历史数据的偏移时间，所有同比环比类算法必须实现该方法。
        偏移量排序类型：ASC(越后面，偏移量越大，数据越早)
        @:return type->list(int/tuple)
        """
        raise NotImplementedError


class RangeRatioAlgorithmsCollection(BasicAlgorithmsCollection, HistoryPointFetcher):
    """
    历史百分比比较算法
    同比环比算法的核心就是算上升/下降占比。
    表达式要求变量：
    value: 当前值
    history_data_point: 历史数据点(DataPoint)
    floor: 下降占比
    ceil: 上升占比
    """

    # 表达式间逻辑关系，同时满足(and)还是满足任一(or)
    expr_op = "or"

    floor_desc_tpl = ""
    ceil_desc_tpl = ""

    def gen_expr(self):
        if self.validated_config["floor"]:
            yield ExprDetectAlgorithms(
                "(unit_convert_min(value, unit) or unit_convert_min(floor_history_value, unit)) "
                "and (unit_convert_min(value, unit) <= "
                "(unit_convert_min(floor_history_value, unit) * (100 - floor) * 0.01))",
                self.floor_desc_tpl,
            )

        if self.validated_config["ceil"]:
            yield ExprDetectAlgorithms(
                "(unit_convert_min(value, unit) or unit_convert_min(ceil_history_value, unit)) "
                "and (unit_convert_min(value, unit) >= "
                "(unit_convert_min(ceil_history_value, unit) * (100 + ceil) * 0.01))",
                self.ceil_desc_tpl,
            )

    def extra_context(self, context):
        env = dict()
        history_data_point = self.history_point_fetcher(context.data_point)
        if history_data_point is None:
            raise HistoryDataNotExists(item_id=context.data_point.item.id, timestamp=context.data_point.timestamp)

        env["history_data_point"] = history_data_point
        env["floor_history_value"] = history_data_point.value
        env["ceil_history_value"] = history_data_point.value
        env.update(self.validated_config)
        return env

    def history_point_fetcher(self, data_point, **kwargs):
        """
        同比环比类算法特有方法，获取历史数据。
        该方法对应`HistoryPointFetcher.query_history_points`
        默认调用`HistoryPointFetcher.fetch_history_point`从缓存中获取
        greed: bool 返回所有offset对应的历史数据
        @return: data_point /list -> [data_point]
        """

        def _fetcher():
            for offset in self.get_history_offsets(data_point.item):
                history_timestamp = data_point.timestamp - offset
                yield self.fetch_history_point(data_point.item, data_point, history_timestamp)

        g = _fetcher()
        if "greed" in kwargs:
            return list(g)
        return next(g)


class SDKPreDetectMixin:
    GROUP_PREDICT_FUNC = None
    PREDICT_FUNC = None

    def detect(self, data_point):
        if data_point.item.query_configs[0]["intelligent_detect"].get("use_sdk", False):
            # 历史依赖准备就绪才开始检测
            if data_point.item.query_configs[0]["intelligent_detect"]["status"] == SDKDetectStatus.PREPARING:
                raise Exception("Strategy history dependency data not ready")

            # 优先从预检测结果中获取检测结果
            if hasattr(self, "_local_pre_detect_results"):
                predict_result_point = self.fetch_pre_detect_result_point(data_point)
                if predict_result_point:
                    return self.detect_by_bkdata(predict_result_point)
                else:
                    raise Exception("Pre detect error.")
            else:
                return self.detect_by_sdk(data_point)
        else:
            return self.detect_by_bkdata(data_point)

    def detect_by_sdk(self, data_point):
        predict_result = self.PREDICT_FUNC(
            data=[{"value": data_point.value, "timestamp": data_point.timestamp * 1000}],
            dimensions=self.generate_dimensions(data_point),
            **self.generate_sdk_predict_params(),
        )
        dimension_fields = getattr(data_point, "dimension_fields", None) or list(data_point.dimensions.keys())

        return self.detect_by_bkdata(
            DataPoint(
                accessed_data={
                    "record_id": data_point.record_id,
                    "value": data_point.value,
                    "values": predict_result[0],
                    "time": int(predict_result[0]["timestamp"] / 1000),
                    "dimensions": data_point.dimensions,
                    "dimension_fields": dimension_fields,
                },
                item=data_point.item,
            )
        )

    def generate_sdk_predict_params(self) -> dict:
        """准备SDK的预测参数

        :return: 预测参数
        """
        return {
            "predict_args": {
                arg_key.lstrip("$"): arg_value for arg_key, arg_value in self.validated_config["args"].items()
            }
        }

    def detect_by_bkdata(self, data_point: DataPoint) -> AnomalyDataPoint:
        """基于bkbase的结果表数据来进行检测."""
        return super().detect(data_point)

    def generate_dimensions(self, data_point: DataPoint) -> dict:
        """生成维度字典.

        :param data_point: 数据点
        :return: 维度字典
        """
        if "agg_dimension" not in data_point.item.query_configs[0]:
            if getattr(data_point, "dimension_fields", None):
                dimensions = {key: data_point.dimensions[key] for key in data_point.dimension_fields}
            else:
                dimensions = copy.deepcopy(data_point.dimensions)
        else:
            dimensions = {key: data_point.dimensions[key] for key in data_point.item.query_configs[0]["agg_dimension"]}
        dimensions["strategy_id"] = int(data_point.item.strategy.id)
        return dimensions

    def pre_detect(self, data_points: list[DataPoint]) -> None:
        """生成按照dimension划分的预测输入数据，调用SDK API进行批量分组预测.

        :param data_points: 待预测的数据
        """
        self._local_pre_detect_results = {}

        item = data_points[0].item
        base_labels = {
            "strategy_id": item.strategy.id,
            "strategy_name": item.strategy.name,
            "bk_biz_id": item.strategy.bk_biz_id,
        }
        if item.query_configs[0]["intelligent_detect"].get("use_sdk", False):
            if item.query_configs[0]["intelligent_detect"]["status"] == SDKDetectStatus.PREPARING:
                logger.info(f"Strategy ({item.strategy.id}) history dependency data not ready")
                return
        else:
            return

        predict_inputs = {}
        for data_point in data_points:
            dimension_md5 = data_point.record_id.split(".")[0]
            if dimension_md5 not in predict_inputs:
                dimensions = self.generate_dimensions(data_point)
                predict_inputs[dimension_md5] = {
                    "dimensions": dimensions,
                    "data": [],
                }

            predict_inputs[dimension_md5]["data"].append(
                {
                    "__index__": data_point.record_id,
                    "value": data_point.value,
                    "timestamp": data_point.timestamp * 1000,
                }
            )

        # 统计每个策略处理的维度数量
        dimension_count = len(predict_inputs)
        metrics.AIOPS_DETECT_DIMENSION_COUNT.labels(**base_labels).set(dimension_count)

        start_time = time.time()
        tasks = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=settings.AIOPS_SDK_PREDICT_CONCURRENCY) as executor:
            for predict_input in predict_inputs.values():
                tasks.append(
                    executor.submit(
                        self.PREDICT_FUNC,
                        **predict_input,
                        interval=int(data_points[0].item.query_configs[0]["agg_interval"]),
                        **self.generate_sdk_predict_params(),
                    )
                )

        error_counter = Counter()
        for future in concurrent.futures.as_completed(tasks):
            try:
                predict_result = future.result()
                for output_data in predict_result:
                    self._local_pre_detect_results[output_data["__index__"]] = output_data
            except Exception as e:
                # 统计检测异常的策略
                if isinstance(getattr(e, "data", None), dict) and "code" in e.data:
                    error_counter[e.data["code"]] += 1
                else:
                    error_counter[e.__class__.__name__] += 1
                logger.warning(f"Predict error: {e}")

        metrics.AIOPS_PRE_DETECT_LATENCY.labels(**base_labels).set(time.time() - start_time)
        total_error_count = 0
        for error_code, count in error_counter.items():
            total_error_count += count
            metrics.AIOPS_DETECT_ERROR_COUNT.labels(**base_labels, error_code=error_code).set(count)

        if dimension_count > 0:
            metrics.AIOPS_DETECT_INVALID_DIMENSION_RATE.labels(**base_labels).set(total_error_count / dimension_count)

        metrics.report_all()

    def fetch_pre_detect_result_point(self, data_point, **kwargs) -> DataPoint:
        """从预检测结果中获取检测输入的结果

        :param data_point: 检测输入数据
        :return: 检测结果
        """
        local_pre_detect_results = getattr(self, "_local_pre_detect_results", {})
        predict_result = local_pre_detect_results.get(data_point.record_id, {})
        dimension_fields = getattr(data_point, "dimension_fields", None) or list(data_point.dimensions.keys())

        if predict_result:
            return DataPoint(
                accessed_data={
                    "record_id": data_point.record_id,
                    "value": predict_result["value"],
                    "values": predict_result,
                    "time": int(predict_result["timestamp"] / 1000),
                    "dimensions": data_point.dimensions,
                    "dimension_fields": dimension_fields,
                },
                item=data_point.item,
            )
        else:
            return None


def adapter_data_access_2_detect(data_record, item):
    data_record.clean()
    return DataPoint(data_record.data, item)
