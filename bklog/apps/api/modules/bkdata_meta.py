"""
Tencent is pleased to support the open source community by making BK-LOG 蓝鲸日志平台 available.
Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
BK-LOG 蓝鲸日志平台 is licensed under the MIT License.
License for BK-LOG 蓝鲸日志平台:
--------------------------------------------------------------------
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial
portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT
LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN
NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
We undertake not to change the open source license (MIT license) applicable to the current version of
the project delivered to anyone in the future.
"""

from django.utils.translation import gettext_lazy as _

from apps.api.base import DataDRFAPISet, DRFActionAPI
from apps.api.modules.utils import (
    add_esb_info_before_request_for_bkdata_user,
    biz_to_tenant_getter,
)
from config.domains import META_APIGATEWAY_ROOT


class _BkDataMetaApi:
    MODULE = _("计算平台元数据模块")

    def __init__(self):
        self.result_tables = DataDRFAPISet(
            url=META_APIGATEWAY_ROOT + "result_tables/",
            module=self.MODULE,
            primary_key="result_table_id",
            description="结果表操作",
            default_return_value=None,
            before_request=add_esb_info_before_request_for_bkdata_user,
            custom_config={
                "storages": DRFActionAPI(method="GET"),
                "mine": DRFActionAPI(method="GET", detail=False),
                "fields": DRFActionAPI(method="GET"),
            },
            bk_tenant_id=biz_to_tenant_getter(
                lambda p: p["bk_biz_id"]
                if "bk_biz_id" in p
                else p["result_table_id"].split("_", 1)[0]
                if "result_table_id" in p
                else p["result_table_ids"][0].split("_", 1)[0],
            ),
        )
