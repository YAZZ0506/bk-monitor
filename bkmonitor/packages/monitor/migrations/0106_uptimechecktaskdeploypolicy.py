from django.db import migrations, models

import bkmonitor.utils.db.fields


class Migration(migrations.Migration):
    dependencies = [
        ("monitor", "0105_uptimechecknode_bk_tenant_id"),
    ]

    operations = [
        migrations.CreateModel(
            name="UptimeCheckTaskDeployPolicy",
            fields=[
                ("id", models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name="ID")),
                ("uptimecheck_id", models.IntegerField(unique=True, verbose_name="拨测任务id")),
                ("bk_tenant_id", models.CharField(default="system", max_length=128, verbose_name="租户ID")),
                ("deploy_policy_id", models.BigIntegerField(default=None, null=True, verbose_name="部署策略ID")),
                ("policy_name", models.CharField(max_length=255, verbose_name="部署策略名称")),
                (
                    "last_workflow_id",
                    models.CharField(blank=True, default="", max_length=128, verbose_name="最近一次执行的工作流ID"),
                ),
                (
                    "bk_host_ids",
                    bkmonitor.utils.db.fields.JsonField(default=list, verbose_name="最近一次执行的下发主机"),
                ),
                ("ensure_absent", models.BooleanField(default=False, verbose_name="最近一次声明是否为反相")),
                ("create_time", models.DateTimeField(auto_now_add=True, verbose_name="创建时间")),
                ("update_time", models.DateTimeField(auto_now=True, verbose_name="更新时间")),
            ],
        ),
    ]
