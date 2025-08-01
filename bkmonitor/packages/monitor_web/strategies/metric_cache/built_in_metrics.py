# 主机内置指标
from typing import Any

SYSTEM_HOST_METRICS: list[dict[str, Any]] = [
    {
        "result_table_label": "os",
        "result_table_id": "system.cpu_detail",
        "result_table_name": "单核CPU",
        "data_label": "",
        "metrics": [
            {"metric_field": "guest", "metric_field_name": "内核在虚拟机上运行的CPU占比", "unit": "percentunit"},
            {"metric_field": "idle", "metric_field_name": "CPU单核空闲率", "unit": "percentunit"},
            {"metric_field": "interrupt", "metric_field_name": "硬件中断数的CPU占比", "unit": "percentunit"},
            {"metric_field": "iowait", "metric_field_name": "CPU单核等待IO的时间占比", "unit": "percentunit"},
            {"metric_field": "nice", "metric_field_name": "低优先级程序在用户态执行的CPU占比", "unit": "percentunit"},
            {"metric_field": "softirq", "metric_field_name": "软件中断数的CPU占比", "unit": "percentunit"},
            {"metric_field": "stolen", "metric_field_name": "CPU单核分配给虚拟机的时间占比", "unit": "percentunit"},
            {"metric_field": "system", "metric_field_name": "CPU单核系统程序使用占比", "unit": "percentunit"},
            {"metric_field": "usage", "metric_field_name": "CPU单核使用率", "unit": "percent"},
            {"metric_field": "user", "metric_field_name": "CPU单核用户程序使用占比", "unit": "percentunit"},
        ],
        "dimensions": [
            {"id": "bk_agent_id", "name": "Agent ID"},
            {"id": "bk_biz_id", "name": "业务ID"},
            {"id": "bk_cloud_id", "name": "采集器云区域ID"},
            {"id": "bk_host_id", "name": "采集主机ID"},
            {"id": "bk_target_cloud_id", "name": "云区域ID"},
            {"id": "bk_target_host_id", "name": "目标主机ID"},
            {"id": "bk_target_ip", "name": "目标IP"},
            {"id": "device_name", "name": "设备名"},
            {"id": "hostname", "name": "主机名"},
            {"id": "ip", "name": "采集器IP"},
        ],
        "default_dimensions": ["bk_target_ip", "bk_target_cloud_id", "device_name"],
    },
    {
        "result_table_label": "os",
        "result_table_id": "system.cpu_summary",
        "result_table_name": "CPU",
        "data_label": "",
        "metrics": [
            {"metric_field": "guest", "metric_field_name": "内核在虚拟机上运行的CPU占比", "unit": "percentunit"},
            {"metric_field": "idle", "metric_field_name": "CPU空闲率", "unit": "percentunit"},
            {"metric_field": "interrupt", "metric_field_name": "硬件中断数的CPU占比", "unit": "percentunit"},
            {"metric_field": "iowait", "metric_field_name": "CPU等待IO的时间占比", "unit": "percentunit"},
            {"metric_field": "nice", "metric_field_name": "低优先级程序在用户态执行的CPU占比", "unit": "percentunit"},
            {"metric_field": "softirq", "metric_field_name": "软件中断数的CPU占比", "unit": "percentunit"},
            {"metric_field": "stolen", "metric_field_name": "CPU分配给虚拟机的时间占比", "unit": "percentunit"},
            {"metric_field": "system", "metric_field_name": "CPU系统程序使用占比", "unit": "percentunit"},
            {"metric_field": "usage", "metric_field_name": "CPU使用率", "unit": "percent"},
            {"metric_field": "user", "metric_field_name": "CPU用户程序使用占比", "unit": "percentunit"},
        ],
        "dimensions": [
            {"id": "bk_agent_id", "name": "Agent ID"},
            {"id": "bk_biz_id", "name": "业务ID"},
            {"id": "bk_cloud_id", "name": "采集器云区域ID"},
            {"id": "bk_host_id", "name": "采集主机ID"},
            {"id": "bk_target_cloud_id", "name": "云区域ID"},
            {"id": "bk_target_host_id", "name": "目标主机ID"},
            {"id": "bk_target_ip", "name": "目标IP"},
            {"id": "device_name", "name": "设备名"},
            {"id": "hostname", "name": "主机名"},
            {"id": "ip", "name": "采集器IP"},
        ],
        "default_dimensions": ["bk_target_ip", "bk_target_cloud_id"],
    },
    {
        "result_table_label": "os",
        "result_table_id": "system.disk",
        "result_table_name": "磁盘",
        "data_label": "",
        "metrics": [
            {"metric_field": "free", "metric_field_name": "磁盘可用空间大小", "unit": "bytes"},
            {"metric_field": "in_use", "metric_field_name": "磁盘空间使用率", "unit": "percent"},
            {"metric_field": "total", "metric_field_name": "磁盘总空间大小", "unit": "bytes"},
            {"metric_field": "used", "metric_field_name": "磁盘已用空间大小", "unit": "bytes"},
        ],
        "dimensions": [
            {"id": "bk_agent_id", "name": "Agent ID"},
            {"id": "bk_biz_id", "name": "业务ID"},
            {"id": "bk_cloud_id", "name": "采集器云区域ID"},
            {"id": "bk_host_id", "name": "采集主机ID"},
            {"id": "bk_target_cloud_id", "name": "云区域ID"},
            {"id": "bk_target_host_id", "name": "目标主机ID"},
            {"id": "bk_target_ip", "name": "目标IP"},
            {"id": "device_name", "name": "设备名"},
            {"id": "device_type", "name": "设备类型"},
            {"id": "hostname", "name": "主机名"},
            {"id": "ip", "name": "采集器IP"},
            {"id": "mount_point", "name": "挂载点"},
        ],
        "default_dimensions": ["bk_target_ip", "bk_target_cloud_id", "mount_point"],
    },
    {
        "result_table_label": "os",
        "result_table_id": "system.env",
        "result_table_name": "系统环境",
        "data_label": "",
        "metrics": [
            {"metric_field": "login_user", "metric_field_name": "登录的用户数", "unit": "short"},
            {"metric_field": "maxfiles", "metric_field_name": "最大文件描述符", "unit": "short"},
            {"metric_field": "procs", "metric_field_name": "系统总进程数", "unit": "short"},
            {
                "metric_field": "procs_blocked_current",
                "metric_field_name": "处于等待I/O完成的进程个数",
                "unit": "short",
            },
            {"metric_field": "procs_ctxt_total", "metric_field_name": "系统上下文切换次数", "unit": "short"},
            {
                "metric_field": "procs_processes_total",
                "metric_field_name": "系统启动后所创建过的进程数量",
                "unit": "short",
            },
            {"metric_field": "proc_running_current", "metric_field_name": "正在运行的进程总个数", "unit": "short"},
            {"metric_field": "uptime", "metric_field_name": "系统启动时间", "unit": "s"},
            {"metric_field": "procs_zombie", "metric_field_name": "僵尸进程数量", "unit": ""},
            {"metric_field": "allocated_files", "metric_field_name": "已分配文件描述符", "unit": "short"},
        ],
        "dimensions": [
            {"id": "bk_agent_id", "name": "Agent ID"},
            {"id": "bk_biz_id", "name": "业务ID"},
            {"id": "bk_cloud_id", "name": "采集器云区域ID"},
            {"id": "bk_host_id", "name": "采集主机ID"},
            {"id": "bk_target_cloud_id", "name": "云区域ID"},
            {"id": "bk_target_host_id", "name": "目标主机ID"},
            {"id": "bk_target_ip", "name": "目标IP"},
            {"id": "city", "name": "时区城市"},
            {"id": "hostname", "name": "主机名"},
            {"id": "ip", "name": "采集器IP"},
            {"id": "timezone", "name": "机器时区"},
        ],
        "default_dimensions": ["bk_target_ip", "bk_target_cloud_id"],
    },
    {
        "result_table_label": "os",
        "result_table_id": "system.inode",
        "result_table_name": "文件句柄",
        "data_label": "",
        "metrics": [
            {"metric_field": "free", "metric_field_name": "可用inode数量", "unit": "short"},
            {"metric_field": "in_use", "metric_field_name": "已用inode占比", "unit": "percent"},
            {"metric_field": "total", "metric_field_name": "总inode数量", "unit": "short"},
            {"metric_field": "used", "metric_field_name": "已用inode数量", "unit": "short"},
        ],
        "dimensions": [
            {"id": "bk_agent_id", "name": "Agent ID"},
            {"id": "bk_biz_id", "name": "业务ID"},
            {"id": "bk_cloud_id", "name": "采集器云区域ID"},
            {"id": "bk_host_id", "name": "采集主机ID"},
            {"id": "bk_target_cloud_id", "name": "云区域ID"},
            {"id": "bk_target_host_id", "name": "目标主机ID"},
            {"id": "bk_target_ip", "name": "目标IP"},
            {"id": "device_name", "name": "设备名"},
            {"id": "hostname", "name": "主机名"},
            {"id": "ip", "name": "采集器IP"},
            {"id": "mountpoint", "name": "mountpoint"},
        ],
        "default_dimensions": ["bk_target_ip", "bk_target_cloud_id"],
    },
    {
        "result_table_label": "os",
        "result_table_id": "system.io",
        "result_table_name": "磁盘I/O",
        "data_label": "",
        "metrics": [
            {"metric_field": "avgqu_sz", "metric_field_name": "平均I/O队列长度", "unit": "Sectors"},
            {"metric_field": "avgrq_sz", "metric_field_name": "设备每次I/O平均数据大小", "unit": "Sectors/IORequest"},
            {"metric_field": "await", "metric_field_name": "I/O平均等待时长", "unit": "ms"},
            {"metric_field": "rkb_s", "metric_field_name": "I/O读速率", "unit": "KBs"},
            {"metric_field": "r_s", "metric_field_name": "I/O读次数", "unit": "rps"},
            {"metric_field": "svctm", "metric_field_name": "I/O平均服务时长", "unit": "ms"},
            {"metric_field": "util", "metric_field_name": "I/O使用率", "unit": "percentunit"},
            {"metric_field": "wkb_s", "metric_field_name": "I/O写速率", "unit": "KBs"},
            {"metric_field": "w_s", "metric_field_name": "I/O写次数", "unit": "wps"},
        ],
        "dimensions": [
            {"id": "bk_agent_id", "name": "Agent ID"},
            {"id": "bk_biz_id", "name": "业务ID"},
            {"id": "bk_cloud_id", "name": "采集器云区域ID"},
            {"id": "bk_host_id", "name": "采集主机ID"},
            {"id": "bk_target_cloud_id", "name": "云区域ID"},
            {"id": "bk_target_host_id", "name": "目标主机ID"},
            {"id": "bk_target_ip", "name": "目标IP"},
            {"id": "device_name", "name": "设备名"},
            {"id": "hostname", "name": "主机名"},
            {"id": "ip", "name": "采集器IP"},
        ],
        "default_dimensions": ["bk_target_ip", "bk_target_cloud_id", "device_name"],
    },
    {
        "result_table_label": "os",
        "result_table_id": "system.load",
        "result_table_name": "负载",
        "data_label": "",
        "metrics": [
            {"metric_field": "load1", "metric_field_name": "1分钟平均负载", "unit": "none"},
            {"metric_field": "load15", "metric_field_name": "15分钟平均负载", "unit": "none"},
            {"metric_field": "load5", "metric_field_name": "5分钟平均负载", "unit": "none"},
            {"metric_field": "per_cpu_load", "metric_field_name": "单核CPU的load", "unit": "none"},
        ],
        "dimensions": [
            {"id": "bk_agent_id", "name": "Agent ID"},
            {"id": "bk_biz_id", "name": "业务ID"},
            {"id": "bk_cloud_id", "name": "采集器云区域ID"},
            {"id": "bk_host_id", "name": "采集主机ID"},
            {"id": "bk_target_cloud_id", "name": "云区域ID"},
            {"id": "bk_target_host_id", "name": "目标主机ID"},
            {"id": "bk_target_ip", "name": "目标IP"},
            {"id": "hostname", "name": "主机名"},
            {"id": "ip", "name": "采集器IP"},
        ],
        "default_dimensions": ["bk_target_ip", "bk_target_cloud_id"],
    },
    {
        "result_table_label": "os",
        "result_table_id": "system.mem",
        "result_table_name": "内存",
        "data_label": "",
        "metrics": [
            {"metric_field": "buffer", "metric_field_name": "内存buffered大小", "unit": "bytes"},
            {"metric_field": "cached", "metric_field_name": "内存cached大小", "unit": "bytes"},
            {"metric_field": "free", "metric_field_name": "物理内存空闲量", "unit": "bytes"},
            {"metric_field": "pct_usable", "metric_field_name": "应用程序内存可用率", "unit": "percent"},
            {"metric_field": "pct_used", "metric_field_name": "应用程序内存使用占比", "unit": "percent"},
            {"metric_field": "psc_pct_used", "metric_field_name": "物理内存已用占比", "unit": "percent"},
            {"metric_field": "psc_used", "metric_field_name": "物理内存已用量", "unit": "bytes"},
            {"metric_field": "shared", "metric_field_name": "共享内存使用量", "unit": "decbytes"},
            {"metric_field": "total", "metric_field_name": "物理内存总大小", "unit": "bytes"},
            {"metric_field": "usable", "metric_field_name": "应用程序内存可用量", "unit": "bytes"},
            {"metric_field": "used", "metric_field_name": "应用程序内存使用量", "unit": "bytes"},
        ],
        "dimensions": [
            {"id": "bk_agent_id", "name": "Agent ID"},
            {"id": "bk_biz_id", "name": "业务ID"},
            {"id": "bk_cloud_id", "name": "采集器云区域ID"},
            {"id": "bk_host_id", "name": "采集主机ID"},
            {"id": "bk_target_cloud_id", "name": "云区域ID"},
            {"id": "bk_target_host_id", "name": "目标主机ID"},
            {"id": "bk_target_ip", "name": "目标IP"},
            {"id": "hostname", "name": "主机名"},
            {"id": "ip", "name": "采集器IP"},
        ],
        "default_dimensions": ["bk_target_ip", "bk_target_cloud_id"],
    },
    {
        "result_table_label": "os",
        "result_table_id": "system.net",
        "result_table_name": "网络",
        "data_label": "",
        "metrics": [
            {"metric_field": "carrier", "metric_field_name": "设备驱动程序检测到的载波丢失数", "unit": "short"},
            {"metric_field": "collisions", "metric_field_name": "网卡冲突包", "unit": "short"},
            {"metric_field": "dropped", "metric_field_name": "网卡丢弃包", "unit": "short"},
            {"metric_field": "errors", "metric_field_name": "网卡错误包", "unit": "short"},
            {"metric_field": "overruns", "metric_field_name": "网卡物理层丢弃", "unit": "short"},
            {"metric_field": "speed_packets_recv", "metric_field_name": "网卡入包量", "unit": "cps"},
            {"metric_field": "speed_packets_sent", "metric_field_name": "网卡出包量", "unit": "cps"},
            {"metric_field": "speed_recv", "metric_field_name": "网卡入流量", "unit": "Bps"},
            {"metric_field": "speed_recv_bit", "metric_field_name": "网卡入流量比特速率", "unit": "bps"},
            {"metric_field": "speed_sent", "metric_field_name": "网卡出流量", "unit": "Bps"},
            {"metric_field": "speed_sent_bit", "metric_field_name": "网卡出流量比特速率 ", "unit": "bps"},
        ],
        "dimensions": [
            {"id": "bk_agent_id", "name": "Agent ID"},
            {"id": "bk_biz_id", "name": "业务ID"},
            {"id": "bk_cloud_id", "name": "采集器云区域ID"},
            {"id": "bk_host_id", "name": "采集主机ID"},
            {"id": "bk_target_cloud_id", "name": "云区域ID"},
            {"id": "bk_target_host_id", "name": "目标主机ID"},
            {"id": "bk_target_ip", "name": "目标IP"},
            {"id": "device_name", "name": "设备名"},
            {"id": "hostname", "name": "主机名"},
            {"id": "ip", "name": "采集器IP"},
        ],
        "default_dimensions": ["bk_target_ip", "bk_target_cloud_id", "device_name"],
    },
    {
        "result_table_label": "os",
        "result_table_id": "system.netstat",
        "result_table_name": "连接状态",
        "data_label": "",
        "metrics": [
            {"metric_field": "cur_tcp_closed", "metric_field_name": "closed连接数", "unit": "short"},
            {"metric_field": "cur_tcp_closewait", "metric_field_name": "closewait连接数", "unit": "short"},
            {"metric_field": "cur_tcp_closing", "metric_field_name": "closing连接数", "unit": "short"},
            {"metric_field": "cur_tcp_estab", "metric_field_name": "estab连接数", "unit": "short"},
            {"metric_field": "cur_tcp_finwait1", "metric_field_name": "finwait1连接数", "unit": "short"},
            {"metric_field": "cur_tcp_finwait2", "metric_field_name": "finwait2连接数", "unit": "short"},
            {"metric_field": "cur_tcp_lastack", "metric_field_name": "lastact连接数", "unit": "short"},
            {"metric_field": "cur_tcp_listen", "metric_field_name": "listen连接数", "unit": "short"},
            {"metric_field": "cur_tcp_syn_recv", "metric_field_name": "synrecv连接数", "unit": "short"},
            {"metric_field": "cur_tcp_syn_sent", "metric_field_name": "synsent连接数", "unit": "short"},
            {"metric_field": "cur_tcp_timewait", "metric_field_name": "timewait连接数", "unit": "short"},
            {"metric_field": "cur_udp_indatagrams", "metric_field_name": "udp接收包量", "unit": "short"},
            {"metric_field": "cur_udp_outdatagrams", "metric_field_name": "udp发送包量", "unit": "short"},
            {"metric_field": "cur_tcp_activeopens", "metric_field_name": "tcp主动发起连接数", "unit": ""},
            {"metric_field": "cur_tcp_passiveopens", "metric_field_name": "tcp被动接受连接数", "unit": ""},
            {"metric_field": "cur_tcp_retranssegs", "metric_field_name": "tcp重传数", "unit": ""},
        ],
        "dimensions": [
            {"id": "bk_agent_id", "name": "Agent ID"},
            {"id": "bk_biz_id", "name": "业务ID"},
            {"id": "bk_cloud_id", "name": "采集器云区域ID"},
            {"id": "bk_host_id", "name": "采集主机ID"},
            {"id": "bk_target_cloud_id", "name": "云区域ID"},
            {"id": "bk_target_host_id", "name": "目标主机ID"},
            {"id": "bk_target_ip", "name": "目标IP"},
            {"id": "hostname", "name": "主机名"},
            {"id": "ip", "name": "采集器IP"},
        ],
        "default_dimensions": ["bk_target_ip", "bk_target_cloud_id"],
    },
    {
        "result_table_label": "host_process",
        "result_table_id": "system.proc",
        "result_table_name": "进程",
        "data_label": "",
        "metrics": [
            {"metric_field": "cpu_system", "metric_field_name": "进程占用系统态时间", "unit": "s"},
            {"metric_field": "cpu_total_ticks", "metric_field_name": "整体占用时间", "unit": "s"},
            {"metric_field": "cpu_usage_pct", "metric_field_name": "进程CPU使用率", "unit": "percentunit"},
            {"metric_field": "cpu_user", "metric_field_name": "进程占用用户态时间", "unit": "s"},
            {"metric_field": "fd_limit_hard", "metric_field_name": "fd_limit_hard", "unit": "short"},
            {"metric_field": "fd_limit_soft", "metric_field_name": "fd_limit_soft", "unit": "short"},
            {"metric_field": "fd_num", "metric_field_name": "进程文件句柄数", "unit": "short"},
            {"metric_field": "io_read_bytes", "metric_field_name": "进程io累计读", "unit": "bytes"},
            {"metric_field": "io_read_speed", "metric_field_name": "进程io读速率", "unit": "Bps"},
            {"metric_field": "io_write_bytes", "metric_field_name": "进程io累计写", "unit": "bytes"},
            {"metric_field": "io_write_speed", "metric_field_name": "进程io写速率", "unit": "Bps"},
            {"metric_field": "mem_res", "metric_field_name": "进程使用物理内存", "unit": "bytes"},
            {"metric_field": "mem_usage_pct", "metric_field_name": "进程内存使用率", "unit": "percentunit"},
            {"metric_field": "mem_virt", "metric_field_name": "进程使用虚拟内存", "unit": "bytes"},
            {"metric_field": "uptime", "metric_field_name": "进程运行时间", "unit": "s"},
        ],
        "dimensions": [
            {"id": "bk_agent_id", "name": "Agent ID"},
            {"id": "bk_biz_id", "name": "业务ID"},
            {"id": "bk_cloud_id", "name": "采集器云区域ID"},
            {"id": "bk_host_id", "name": "采集主机ID"},
            {"id": "bk_target_cloud_id", "name": "云区域ID"},
            {"id": "bk_target_host_id", "name": "目标主机ID"},
            {"id": "bk_target_ip", "name": "目标IP"},
            {"id": "display_name", "name": "进程显示名称"},
            {"id": "hostname", "name": "主机名"},
            {"id": "ip", "name": "采集器IP"},
            {"id": "param_regex", "name": "进程匹配参数正则"},
            {"id": "pgid", "name": "进程组id"},
            {"id": "pid", "name": "进程id"},
            {"id": "port", "name": "进程监听端口"},
            {"id": "ppid", "name": "父进程ID"},
            {"id": "proc_name", "name": "进程二进制文件名称"},
            {"id": "state", "name": "进程状态"},
            {"id": "username", "name": "进程用户名"},
        ],
        "default_dimensions": ["bk_target_ip", "bk_target_cloud_id", "display_name"],
    },
    {
        "result_table_label": "os",
        "result_table_id": "system.swap",
        "result_table_name": "交换分区",
        "data_label": "",
        "metrics": [
            {"metric_field": "free", "metric_field_name": "SWAP空闲量", "unit": "bytes"},
            {"metric_field": "pct_used", "metric_field_name": "SWAP已用占比", "unit": "percent"},
            {"metric_field": "swap_in", "metric_field_name": "swap从硬盘到内存", "unit": "KBs"},
            {"metric_field": "swap_out", "metric_field_name": "swap从内存到硬盘", "unit": "KBs"},
            {"metric_field": "total", "metric_field_name": "SWAP总量", "unit": "bytes"},
            {"metric_field": "used", "metric_field_name": "SWAP已用量", "unit": "bytes"},
        ],
        "dimensions": [
            {"id": "bk_agent_id", "name": "Agent ID"},
            {"id": "bk_biz_id", "name": "业务ID"},
            {"id": "bk_cloud_id", "name": "采集器云区域ID"},
            {"id": "bk_host_id", "name": "采集主机ID"},
            {"id": "bk_target_cloud_id", "name": "云区域ID"},
            {"id": "bk_target_host_id", "name": "目标主机ID"},
            {"id": "bk_target_ip", "name": "目标IP"},
            {"id": "hostname", "name": "主机名"},
            {"id": "ip", "name": "采集器IP"},
        ],
        "default_dimensions": ["bk_target_ip", "bk_target_cloud_id"],
    },
]


# 拨测内置指标
UPTIMECHECK_METRICS: list[dict[str, Any]] = [
    {
        "result_table_id": "uptimecheck.http",
        "result_table_name": "HTTP",
        "data_label": "uptimecheck_http",
        "metrics": [
            {
                "metric_field": "available",
                "metric_field_name": "HTTP 单点可用率",
                "unit": "percentunit",
                "dimensions": [
                    {"id": "task_id", "name": "任务ID"},
                    {"id": "node_id", "name": "节点ID"},
                    {"id": "url", "name": "目标"},
                ],
            },
            {
                "metric_field": "task_duration",
                "metric_field_name": "HTTP 响应时间",
                "unit": "ms",
                "dimensions": [
                    {"id": "task_id", "name": "任务ID"},
                    {"id": "node_id", "name": "节点ID"},
                    {"id": "url", "name": "目标"},
                ],
            },
            {
                "metric_field": "response_code",
                "metric_field_name": "HTTP 期望响应码",
                "unit": "",
                "dimensions": [
                    {"id": "task_id", "name": "任务ID"},
                    {"id": "url", "name": "目标"},
                ],
            },
            {
                "metric_field": "message",
                "metric_field_name": "HTTP 期望响应内容",
                "unit": "",
                "dimensions": [
                    {"id": "task_id", "name": "任务ID"},
                    {"id": "url", "name": "目标"},
                ],
            },
        ],
    },
    {
        "result_table_id": "uptimecheck.icmp",
        "result_table_name": "ICMP",
        "data_label": "uptimecheck_icmp",
        "metrics": [
            {
                "metric_field": "available",
                "metric_field_name": "ICMP 单点可用率",
                "unit": "percentunit",
                "dimensions": [
                    {"id": "bk_agent_id", "name": "Agent ID"},
                    {"id": "bk_host_id", "name": "采集主机ID"},
                    {"id": "bk_target_host_id", "name": "目标主机ID"},
                    {"id": "node_id", "name": "节点ID"},
                    {"id": "resolved_ip", "name": "请求域名解析结果IP"},
                    {"id": "target", "name": "目标地址"},
                    {"id": "target_type", "name": "目标地址类型"},
                    {"id": "task_id", "name": "任务id"},
                ],
            },
            {
                "metric_field": "avg_rtt",
                "metric_field_name": "ICMP 平均时延",
                "unit": "ms",
                "dimensions": [
                    {"id": "bk_agent_id", "name": "Agent ID"},
                    {"id": "bk_host_id", "name": "采集主机ID"},
                    {"id": "bk_target_host_id", "name": "目标主机ID"},
                    {"id": "node_id", "name": "节点ID"},
                    {"id": "resolved_ip", "name": "请求域名解析结果IP"},
                    {"id": "target", "name": "目标地址"},
                    {"id": "target_type", "name": "目标地址类型"},
                    {"id": "task_id", "name": "任务id"},
                ],
            },
            {
                "metric_field": "loss_percent",
                "metric_field_name": "ICMP 丢包率",
                "unit": "percentunit",
                "dimensions": [
                    {"id": "bk_agent_id", "name": "Agent ID"},
                    {"id": "bk_host_id", "name": "采集主机ID"},
                    {"id": "bk_target_host_id", "name": "目标主机ID"},
                    {"id": "node_id", "name": "节点ID"},
                    {"id": "resolved_ip", "name": "请求域名解析结果IP"},
                    {"id": "target", "name": "目标地址"},
                    {"id": "target_type", "name": "目标地址类型"},
                    {"id": "task_id", "name": "任务id"},
                ],
            },
            {
                "metric_field": "max_rtt",
                "metric_field_name": "ICMP 最大时延",
                "unit": "ms",
                "dimensions": [
                    {"id": "bk_agent_id", "name": "Agent ID"},
                    {"id": "bk_host_id", "name": "采集主机ID"},
                    {"id": "bk_target_host_id", "name": "目标主机ID"},
                    {"id": "node_id", "name": "节点ID"},
                    {"id": "resolved_ip", "name": "请求域名解析结果IP"},
                    {"id": "target", "name": "目标地址"},
                    {"id": "target_type", "name": "目标地址类型"},
                    {"id": "task_id", "name": "任务id"},
                ],
            },
            {
                "metric_field": "min_rtt",
                "metric_field_name": "ICMP 最小时延",
                "unit": "ms",
                "dimensions": [
                    {"id": "bk_agent_id", "name": "Agent ID"},
                    {"id": "bk_host_id", "name": "采集主机ID"},
                    {"id": "bk_target_host_id", "name": "目标主机ID"},
                    {"id": "node_id", "name": "节点ID"},
                    {"id": "resolved_ip", "name": "请求域名解析结果IP"},
                    {"id": "target", "name": "目标地址"},
                    {"id": "target_type", "name": "目标地址类型"},
                    {"id": "task_id", "name": "任务id"},
                ],
            },
            {
                "metric_field": "task_duration",
                "metric_field_name": "ICMP 响应时间",
                "unit": "ms",
                "dimensions": [
                    {"id": "bk_agent_id", "name": "Agent ID"},
                    {"id": "bk_host_id", "name": "采集主机ID"},
                    {"id": "bk_target_host_id", "name": "目标主机ID"},
                    {"id": "node_id", "name": "节点ID"},
                    {"id": "resolved_ip", "name": "请求域名解析结果IP"},
                    {"id": "target", "name": "目标地址"},
                    {"id": "target_type", "name": "目标地址类型"},
                    {"id": "task_id", "name": "任务id"},
                ],
            },
        ],
    },
    {
        "result_table_id": "uptimecheck.tcp",
        "result_table_name": "TCP",
        "data_label": "uptimecheck_tcp",
        "metrics": [
            {
                "metric_field": "available",
                "metric_field_name": "TCP 单点可用率",
                "unit": "percentunit",
                "dimensions": [
                    {"id": "task_id", "name": "任务ID"},
                    {"id": "node_id", "name": "节点ID"},
                    {"id": "target_host", "name": "目标IP"},
                    {"id": "target_port", "name": "目标端口"},
                ],
            },
            {
                "metric_field": "task_duration",
                "metric_field_name": "TCP 响应时间",
                "unit": "ms",
                "dimensions": [
                    {"id": "task_id", "name": "任务ID"},
                    {"id": "node_id", "name": "节点ID"},
                    {"id": "target_host", "name": "目标IP"},
                    {"id": "target_port", "name": "目标端口"},
                ],
            },
        ],
    },
    {
        "result_table_id": "uptimecheck.udp",
        "result_table_name": "UDP",
        "data_label": "uptimecheck_udp",
        "metrics": [
            {
                "metric_field": "available",
                "metric_field_name": "UDP 单点可用率",
                "unit": "percentunit",
                "dimensions": [
                    {"id": "task_id", "name": "任务ID"},
                    {"id": "node_id", "name": "节点ID"},
                    {"id": "target_host", "name": "目标IP"},
                    {"id": "target_port", "name": "目标端口"},
                ],
            },
            {
                "metric_field": "task_duration",
                "metric_field_name": "UDP 响应时间",
                "unit": "ms",
                "dimensions": [
                    {"id": "task_id", "name": "任务ID"},
                    {"id": "node_id", "name": "节点ID"},
                    {"id": "target_host", "name": "目标IP"},
                    {"id": "target_port", "name": "目标端口"},
                ],
            },
        ],
    },
]


# 内置进程采集插件
PROCESS_METRICS: list[dict[str, str]] = [
    {"metric_field": "cpu_start_time", "metric_field_name": "进程启动时间", "unit": "none"},
    {"metric_field": "cpu_system", "metric_field_name": "进程占用系统态时间", "unit": "ms"},
    {"metric_field": "cpu_total_pct", "metric_field_name": "进程CPU使用率", "unit": "percentunit"},
    {"metric_field": "cpu_total_ticks", "metric_field_name": "整体占用时间", "unit": "ms"},
    {"metric_field": "cpu_user", "metric_field_name": "进程占用用户态时间", "unit": "ms"},
    {"metric_field": "fd_limit_hard", "metric_field_name": "fd_limit_hard", "unit": "short"},
    {"metric_field": "fd_limit_soft", "metric_field_name": "fd_limit_soft", "unit": "short"},
    {"metric_field": "fd_open", "metric_field_name": "打开的文件描述符数量", "unit": "short"},
    {"metric_field": "io_read_bytes", "metric_field_name": "进程io累计读", "unit": "bytes"},
    {"metric_field": "io_read_speed", "metric_field_name": "进程io读速率", "unit": "Bps"},
    {"metric_field": "io_write_bytes", "metric_field_name": "进程io累计写", "unit": "bytes"},
    {"metric_field": "io_write_speed", "metric_field_name": "进程io写速率", "unit": "Bps"},
    {"metric_field": "memory_rss_bytes", "metric_field_name": "物理内存", "unit": "bytes"},
    {"metric_field": "memory_rss_pct", "metric_field_name": "物理内存使用率", "unit": "percentunit"},
    {"metric_field": "memory_share", "metric_field_name": "共享内存", "unit": "bytes"},
    {"metric_field": "memory_size", "metric_field_name": "虚拟内存", "unit": "bytes"},
    {"metric_field": "alive", "metric_field_name": "端口存活", "unit": "none"},
]

PROCESS_METRIC_DIMENSIONS: list[dict[str, str]] = [
    {"id": "bk_target_ip", "name": "目标IP"},
    {"id": "bk_target_service_category_id", "name": "bk_target_service_category_id"},
    {"id": "bk_target_cloud_id", "name": "目标机器云区域ID"},
    {"id": "bk_biz_id", "name": "业务ID"},
    {"id": "pid", "name": "进程序号"},
    {"id": "process_username", "name": "process_username"},
    {"id": "target", "name": "target"},
    {"id": "dims", "name": "dims"},
    {"id": "bk_target_topo_level", "name": "bk_target_topo_level"},
    {"id": "bk_target_host_id", "name": "bk_target_host_id"},
    {"id": "bk_collect_config_id", "name": "采集配置"},
    {"id": "bk_target_topo_id", "name": "bk_target_topo_id"},
    {"id": "process_name", "name": "进程名"},
]

PROCESS_PORT_METRIC_DIMENSIONS: list[dict[str, str]] = [
    {"id": "dims", "name": "dims"},
    {"id": "bk_collect_config_id", "name": "采集配置"},
    {"id": "bk_target_host_id", "name": "bk_target_host_id"},
    {"id": "bk_biz_id", "name": "业务ID"},
    {"id": "bk_target_topo_id", "name": "bk_target_topo_id"},
    {"id": "listen_address", "name": "监听地址"},
    {"id": "process_name", "name": "进程名"},
    {"id": "bk_target_cloud_id", "name": "目标机器云区域ID"},
    {"id": "bk_target_ip", "name": "目标IP"},
    {"id": "bk_target_topo_level", "name": "bk_target_topo_level"},
    {"id": "target", "name": "target"},
    {"id": "process_username", "name": "process_username"},
    {"id": "listen_port", "name": "监听端口"},
    {"id": "bk_target_service_category_id", "name": "bk_target_service_category_id"},
    {"id": "pid", "name": "进程序号"},
]
