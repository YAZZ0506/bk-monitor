/* eslint-disable @typescript-eslint/naming-convention */
<!--
* Tencent is pleased to support the open source community by making
* 蓝鲸智云PaaS平台 (BlueKing PaaS) available.
*
* Copyright (C) 2021 THL A29 Limited, a Tencent company.  All rights reserved.
*
* 蓝鲸智云PaaS平台 (BlueKing PaaS) is licensed under the MIT License.
*
* License for 蓝鲸智云PaaS平台 (BlueKing PaaS):
*
* ---------------------------------------------------
* Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
* documentation files (the "Software"), to deal in the Software without restriction, including without limitation
* the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
* to permit persons to whom the Software is furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in all copies or substantial portions of
* the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
* THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
* CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
* IN THE SOFTWARE.
-->
<template>
  <ip-selector
    ref="selector"
    :key="selectorKey"
    v-bkloading="{ isLoading }"
    class="ip-selector"
    :active.sync="active"
    :cluster-table-config="templateTableConfig"
    :cluster-template-placeholder="$t('搜索集群名')"
    :custom-input-table-config="staticTableConfig"
    :default-active-name="defaultActiveName"
    :default-checked-nodes="defaultCheckedNodes"
    :dynamic-group-placeholder="$t('搜索动态分组名称')"
    :dynamic-group-table-config="dynamicGroupTableConfig"
    :dynamic-table-config="dynamicTableConfig"
    :get-default-data="handleGetDefaultData"
    :get-default-selections="getDefaultSelections"
    :get-search-result-selections="getSearchResultSelections"
    :get-search-table-data="handleGetSearchTableData"
    :height="height"
    :left-panel-width="leftPanelWidth"
    :panels="panels"
    :preview-data="previewData"
    :preview-operate-list="previewOperateList"
    :preview-range="previewRange"
    :preview-width="previewWidth"
    :result-width="380"
    :service-template-placeholder="$t('搜索模块名')"
    :show-table-tab="showTableTab"
    :static-table-config="staticTableConfig"
    :template-table-config="templateTableConfig"
    :tree-height="treeHeight"
    @check-change="handleCheckChange"
    @menu-click="handleMenuClick"
    @remove-node="handleRemoveNode"
    @search-selection-change="handleSearchSelectionChange"
  />
</template>
<script lang="ts">
import { Component, Emit, Prop, Ref, Vue, Watch } from 'vue-property-decorator';

import {
  getHostInstanceByIp,
  getHostInstanceByNode,
  getNodesByTemplate,
  getServiceInstanceByNode,
  getTemplate,
  getTopoTree,
} from 'monitor-api/modules/commons';
import { executeIpChooserDynamicGroup, groupsIpChooserDynamicGroup } from 'monitor-api/modules/model';
import { copyText } from 'monitor-common/utils/utils';

import { defaultSearch } from '../common/util';
import AgentStatus from '../components/agent-status.vue';
import IpSelector from '../index.vue';

import type { IMenu, IPanel, IPreviewData, IpType, ITableCheckData, ITableConfig } from '../types/selector-type';

@Component({
  name: 'topo-selector',
  components: {
    IpSelector,
  },
})
export default class TopoSelector extends Vue {
  @Prop({ default: 'TOPO' }) private readonly targetNodeType!: IpType;
  @Prop({ default: 'HOST' }) private readonly targetObjectType!: 'HOST' | 'SERVICE';
  @Prop({ default: () => [], type: Array }) private readonly checkedData!: any[];
  @Prop({ default: 280, type: [Number, String] }) private readonly previewWidth!: number | string;
  @Prop({ default: () => [150, 600], type: Array }) private readonly previewRange!: number[];
  @Prop({ default: false }) private readonly hiddenTopo!: boolean;
  @Prop({ default: true }) private readonly hiddenDynamicGroup!: boolean;
  @Prop({ default: 460, type: [Number, String] }) private readonly height!: number | string;
  @Prop({ default: false, type: Boolean }) private readonly withExternalIps!: boolean;
  @Prop({ default: false, type: Boolean }) private readonly hiddenTemplate!: boolean;
  @Prop({ default: 240, type: [Number, String] }) private readonly leftPanelWidth!: number | string;
  @Prop({ default: false, type: Boolean }) private readonly showTableTab!: boolean;
  @Prop({ default: 300, type: [Number, String] }) private readonly treeHeight: number | string;

  @Ref('selector') private readonly selector!: IpSelector;

  private isLoading = false;
  private currentTargetNodeType = this.targetNodeType;
  // 当前激活tab
  private active = '';
  // topo树数据
  private topoTree: any[] = [];
  private ipNodesMap: any = {};
  // 动态拓扑表格数据
  private topoTableData: any[] = [];
  // 动态TOPO树默认勾选数据
  private defaultCheckedNodes: (number | string)[] = [];
  // 静态表格数据
  private staticTableData: any[] = [];
  // 动态拓扑表格配置
  private dynamicTableConfig: ITableConfig[] = [];
  // 静态拓扑表格配置
  private staticTableConfig: ITableConfig[] = [];
  // 动态分组表格配置
  private dynamicGroupTableConfig: ITableConfig[] = [];
  // 模板拓扑配置
  private templateTableConfig: ITableConfig[] = [];
  // 预览数据
  private previewData: IPreviewData[] = [];
  // v-if方式渲染选择器时需要重置Key
  private selectorKey = 1;
  // 节点类型和实际IP选择类型的映射关系的默认值
  private nodeTypeMap = {
    TOPO: 'dynamic-topo',
    INSTANCE: 'static-topo',
    SERVICE_TEMPLATE: 'service-template',
    SET_TEMPLATE: 'cluster',
    DYNAMIC_GROUP: 'dynamic-group',
  };
  // 默认激活预览面板
  private defaultActiveName = ['TOPO', 'INSTANCE', 'SERVICE_TEMPLATE', 'SET_TEMPLATE', 'DYNAMIC_GROUP'];
  private setTemplateData = [];
  private serviceTemplateData = [];

  private get isInstance() {
    // 采集对象为服务时，只能选择动态
    return this.targetObjectType === 'SERVICE';
  }

  private get panels(): IPanel[] {
    const panels: IPanel[] = [
      {
        name: 'dynamic-topo',
        label: this.$t('动态拓扑'),
        tips: this.$t('不能混用'),
        disabled: false,
        type: 'TOPO',
        hidden: this.hiddenTopo,
      },
      {
        name: 'static-topo',
        label: this.$t('静态拓扑'),
        hidden: this.isInstance,
        tips: this.$t('不能混用'),
        disabled: false,
        type: 'INSTANCE',
      },
      {
        name: 'service-template',
        label: this.$t('服务模板'),
        tips: this.$t('不能混用'),
        disabled: false,
        type: 'TOPO',
        hidden: this.hiddenTopo || this.hiddenTemplate,
      },
      {
        name: 'cluster',
        label: this.$t('集群模板'),
        tips: this.$t('不能混用'),
        disabled: false,
        type: 'TOPO',
        hidden: this.hiddenTopo || this.hiddenTemplate,
      },
      {
        name: 'custom-input',
        label: this.$t('自定义输入'),
        hidden: this.isInstance,
        tips: this.$t('不能混用'),
        disabled: false,
        type: 'INSTANCE',
      },
      {
        name: 'dynamic-group',
        label: this.$t('动态分组'),
        hidden: this.hiddenDynamicGroup || this.isInstance,
        tips: this.$t('不能混用'),
        disabled: false,
        type: 'DYNAMIC_GROUP',
      },
    ];
    const dynamicType = ['TOPO', 'SERVICE_TEMPLATE', 'SET_TEMPLATE', 'DYNAMIC_GROUP'];
    const isDynamic = this.previewData.some(item => dynamicType.includes(item.id) && item.data.length);
    const isStatic = this.previewData.some(item => item.id === 'INSTANCE' && item.data.length);
    return panels.map(item => {
      item.disabled = (item.name !== this.active && isDynamic) || (item.type === 'TOPO' && isStatic);
      return item;
    });
  }

  // 更多操作配置
  private get previewOperateList() {
    return [
      {
        id: 'copyIp',
        label: this.$t('复制IP'),
        hidden: !['static-topo', 'custom-input'].includes(this.active),
      },
      {
        id: 'removeAll',
        label: this.$t('移除所有'),
      },
    ];
  }
  @Watch('active')
  private handleActiveChange() {
    const activeToNodeTypeMap = {
      'dynamic-topo': 'TOPO',
      'static-topo': 'INSTANCE',
      'service-template': 'SERVICE_TEMPLATE',
      cluster: 'SET_TEMPLATE',
      'custom-input': 'INSTANCE',
      'dynamic-group': 'DYNAMIC_GROUP',
    };
    this.currentTargetNodeType = activeToNodeTypeMap[this.active];
    this.staticTableData = [];
    this.topoTableData = [];
    this.setTemplateData = [];
    this.serviceTemplateData = [];
  }

  @Watch('targetNodeType', { immediate: true })
  private handleDefaultActiveChange() {
    this.currentTargetNodeType = this.targetNodeType;
    if (this.nodeTypeMap[this.currentTargetNodeType]) {
      this.active = this.nodeTypeMap[this.currentTargetNodeType];
    } else {
      const panel = this.panels.find(item => !item.disabled && !item.hidden);
      this.active = panel ? panel.name : 'static-topo';
    }
    // 判断当前active的tab是否不可用
    const panel = this.panels.find(item => item.name === this.active);
    if (!panel || panel.hidden || panel.disabled) {
      const firstEnablePanel = this.panels.find(item => !item.hidden && !panel.disabled);
      this.active = firstEnablePanel?.name;
    }
  }

  @Watch('checkedData', { immediate: true })
  private handleCheckedDataChange() {
    const nodeTypeTextMap = {
      TOPO: this.$t('拓扑节点'),
      INSTANCE: 'IP',
      SERVICE_TEMPLATE: this.$t('服务模板'),
      SET_TEMPLATE: this.$t('集群模板'),
      DYNAMIC_GROUP: this.$t('动态分组'),
    };
    const nodeTypeNameMap = {
      TOPO: 'node_path',
      INSTANCE: 'ip',
      SERVICE_TEMPLATE: 'bk_inst_name',
      SET_TEMPLATE: 'bk_inst_name',
      DYNAMIC_GROUP: 'name',
    };
    this.previewData = [];
    this.previewData.push({
      id: this.currentTargetNodeType,
      name: nodeTypeTextMap[this.currentTargetNodeType] || '--',
      data: [...this.checkedData],
      dataNameKey: nodeTypeNameMap[this.currentTargetNodeType],
    });
    this.currentTargetNodeType === 'TOPO' && this.handleSetDefaultCheckedNodes();
  }

  private created() {
    this.dynamicTableConfig = [
      {
        prop: 'node_path',
        label: this.$t('节点名称'),
        minWidth: 100,
      },
      {
        prop: 'count',
        label: this.$t('服务实例'),
        hidden: this.targetObjectType === 'HOST',
      },
      {
        prop: 'status',
        label: this.$t('Agent状态'),
        render: this.renderAgentCountStatus,
      },
      {
        prop: 'labels',
        label: this.$t('分类'),
        render: this.renderLabels,
        minWidth: 100,
      },
    ];
    this.templateTableConfig = [
      {
        prop: 'node_path',
        label: this.$t('节点名称'),
        minWidth: 100,
      },
      {
        prop: 'count',
        label: this.$t('服务实例'),
        hidden: this.targetObjectType === 'HOST',
      },
      {
        prop: 'status',
        label: this.$t('Agent状态'),
        render: this.renderAgentCountStatus,
      },
      {
        prop: 'labels',
        label: this.$t('分类'),
        render: this.renderLabels,
        minWidth: 100,
      },
    ];
    this.staticTableConfig = [
      {
        prop: 'ip',
        label: 'IP',
      },
      {
        prop: 'agent_status',
        label: this.$t('Agent状态'),
        render: this.renderIpAgentStatus,
      },
      {
        prop: 'bk_cloud_name',
        label: this.$t('管控区域'),
      },
      {
        prop: 'bk_os_type',
        label: this.$t('操作系统'),
      },
    ];
    this.dynamicGroupTableConfig = [
      {
        prop: 'ip',
        label: 'IP',
      },
      {
        prop: 'ipv6',
        label: 'ipv6',
      },
      {
        prop: 'cloud_area',
        label: this.$t('管控区域'),
        render: this.renderCloudAreaForDynamicGroup,
      },
      {
        prop: 'os_name',
        label: this.$t('操作系统'),
      },
    ];
  }
  private renderLabels(row: any) {
    const { labels = [] } = row;
    const children = labels.map((item: any) =>
      this.$createElement(
        'span',
        {
          style: {
            background: '#f0f1f5',
            padding: '2px 6px',
            marginRight: '4px',
            height: '20px',
          },
        },
        `${item.first}:${item.second}`
      )
    );
    return Vue.extend({
      render: h => h('div', [...children]),
    });
  }
  private renderIpAgentStatus(row: any) {
    const textMap: any = {
      normal: this.$t('正常'),
      abnormal: this.$t('异常'),
      not_exist: this.$t('未安装'),
    };
    const statusMap: any = {
      normal: 'running',
      abnormal: 'terminated',
      not_exist: 'unknown',
    };
    return Vue.extend({
      render: h =>
        h(AgentStatus, {
          props: {
            type: 1,
            data: [
              {
                status: statusMap[row.agent_status],
                count: row.agent_error_count,
                display: textMap[row.agent_status],
              },
            ],
          },
        }),
    });
  }

  private renderCloudAreaForDynamicGroup(row: any) {
    return Vue.extend({
      render: h => h('span', row?.cloud_area?.name || '--'),
    });
  }

  private renderAgentCountStatus(row: any) {
    return Vue.extend({
      render: h =>
        h(AgentStatus, {
          props: {
            type: 3,
            data: [
              {
                count: row.all_host.length,
                errorCount: row.agent_error_count,
              },
            ],
          },
        }),
    });
  }

  // 获取当前tab下默认数据
  private async handleGetDefaultData() {
    if (['dynamic-topo', 'static-topo'].includes(this.active)) {
      // 动态拓扑默认组件数据
      if (!this.topoTree.length) {
        const data = await this.getTopoTree();
        this.topoTree = this.removeIpNodes(data);
        this.active === 'dynamic-topo' && this.handleSetDefaultCheckedNodes();
      }
      return [
        {
          name: this.$t('根节点'),
          children: this.topoTree,
        },
      ];
    }
    if (['service-template', 'cluster'].includes(this.active)) {
      const data = await getTemplate({
        bk_inst_type: this.isInstance ? 'SERVICE' : 'HOST',
        bk_obj_id: this.active === 'cluster' ? 'SET_TEMPLATE' : 'SERVICE_TEMPLATE',
        with_count: true,
      }).catch(() => ({ children: [] }));
      // 兼容回显数据不带name的情况
      this.handleAddNameProperty(data.children);
      return data.children;
    }
    if (['dynamic-group'].includes(this.active)) {
      const data = await groupsIpChooserDynamicGroup({
        scope_list: [
          {
            scope_type: 'biz',
            scope_id: this.$store.getters.bizId,
          },
        ],
      });
      return data;
    }
  }
  private handleSetDefaultCheckedNodes() {
    // todo 待优化，多处调用，性能问题
    const { data = [] } = this.previewData.find(item => item.id === 'TOPO') || {};
    this.defaultCheckedNodes = this.getCheckedNodesIds(this.topoTree, data);
  }
  // 获取选中节点的ID集合
  private getCheckedNodesIds(nodes: any[], checkedData: any[] = []) {
    if (!checkedData.length) return [];
    return nodes.reduce<(number | string)[]>((pre, item) => {
      const { children = [], bk_inst_id = '', bk_obj_id = '', id } = item;
      const exist = checkedData.some(
        checkedData => checkedData.bk_inst_id === bk_inst_id && checkedData.bk_obj_id === bk_obj_id
      );
      if (exist) {
        // 根节点是业务情况下特殊处理
        // if (bk_obj_id === 'biz' && !this.previewData?.[0]?.data?.[0][this.previewData?.[0]?.dataNameKey]) {
        //   }
        const index = this.previewData?.[0]?.data?.findIndex(set => bk_inst_id === set.bk_inst_id);
        if (typeof index === 'number' && index > -1) {
          if (!this.previewData?.[0]?.data?.[index]?.[this.previewData?.[0]?.dataNameKey]) {
            this.$set(
              this.previewData[0].data[index],
              this.previewData[0].dataNameKey,
              item.name || item.bk_inst_name || ''
            );
          }
        }
        pre.push(id);
      }
      if (children.length) {
        pre.push(...this.getCheckedNodesIds(children, checkedData));
      }
      return pre;
    }, []);
  }
  // 移除树上的所有IP节点
  private removeIpNodes(data: any[]) {
    data.forEach(item => {
      const { children = [] } = item;
      if (item.bk_obj_id === 'module' && children.length) {
        this.ipNodesMap[`${item.bk_inst_id}_${item.bk_obj_id}`] = [...item.children];
        item.children = [];
        // item.ipCount = children.length
      } else if (children.length) {
        this.removeIpNodes(item.children);
        // const data = this.removeIpNodes(item.children)
        // data.forEach((child) => {
        //   item.ipCount = (item.ipCount || 0) + (child.ipCount || 0)
        // })
      }
    });
    return data;
  }
  // 获取树节点下所有IP节点
  private getNodesIpList(data: any[]) {
    return data.reduce<any[]>((pre, item) => {
      if (item.bk_obj_id === 'module') {
        const ipNodes = this.ipNodesMap[`${item.bk_inst_id}_${item.bk_obj_id}`] || [];
        pre.push(...ipNodes);
      } else if (item.children?.length) {
        pre.push(...this.getNodesIpList(item.children));
      }
      return pre;
    }, []);
  }
  // 获取表格数据（组件内部封装了交互逻辑）
  private async handleGetSearchTableData(
    params: any,
    type?: string
  ): Promise<{
    data: any[];
    total: number;
  }> {
    if (this.active === 'dynamic-topo') {
      return await this.getDynamicTopoTableData(params, type);
    }
    if (this.active === 'static-topo') {
      return await this.getStaticTableData(params, type);
    }
    if (['service-template', 'cluster'].includes(this.active)) {
      return await this.getTemplateTableData(params, type);
    }
    if (this.active === 'custom-input') {
      return await this.getCustomInputTableData(params);
    }
    if (this.active === 'dynamic-group') {
      return await this.getDynamicGroupTableData(params);
    }
    return {
      total: 0,
      data: [],
    };
  }
  // 获取动态topo表格数据
  private async getDynamicTopoTableData(params: any, type?: string) {
    const { selections = [], tableKeyword = '', parentNode } = params;
    if (type === 'selection-change') {
      // 如果点击的是叶子节点，则显示叶子节点本身，否则就显示子节点
      const curSelections = !selections.length && parentNode ? [parentNode.data] : selections;
      this.topoTableData =
        this.targetObjectType === 'SERVICE'
          ? await getServiceInstanceByNode({ node_list: curSelections }).catch(() => [])
          : await getHostInstanceByNode({ node_list: curSelections }).catch(() => []);
    }
    const data = defaultSearch(this.topoTableData, tableKeyword);

    return {
      total: data.length,
      data,
    };
  }
  // 获取静态表格数据
  private async getStaticTableData(params: any, type?: string) {
    const { selections = [], tableKeyword = '' } = params;
    if (type === 'selection-change' && !!selections.length) {
      const list = this.getNodesIpList(selections);
      let i = 0;
      const len = list.length;
      const nodeMap = {};
      while (i < len) {
        const { bk_cloud_id, ip } = list[i];
        const key = bk_cloud_id === undefined ? `${ip}|${bk_cloud_id}` : ip;
        nodeMap[key] = { bk_cloud_id, ip };
        i += 1;
      }
      const ipNodes = Object.values(nodeMap);
      this.staticTableData = await getHostInstanceByIp({
        ip_list: ipNodes,
      }).catch(() => []);
    }
    const data = defaultSearch(this.staticTableData, tableKeyword);
    return {
      total: data.length,
      data,
    };
  }
  // 获取模板类表格数据
  private async getTemplateTableData(params: any, type?: string) {
    const { selections = [], tableKeyword = '' } = params;
    if (type === 'selection-change' && !!selections.length) {
      const data = await getNodesByTemplate({
        bk_inst_type: this.isInstance ? 'SERVICE' : 'HOST',
        bk_obj_id: this.active === 'cluster' ? 'SET_TEMPLATE' : 'SERVICE_TEMPLATE',
        bk_inst_ids: selections.map((item: any) => item.bk_inst_id),
      }).catch(() => []);
      this.active === 'cluster' ? (this.setTemplateData = data) : (this.serviceTemplateData = data);
    }
    const data = defaultSearch(
      this.active === 'cluster' ? this.setTemplateData : this.serviceTemplateData,
      tableKeyword
    );

    return {
      total: data.length,
      data,
    };
  }
  // 兼容服务模板和集群模板接口数据不带name的情况
  private handleAddNameProperty(templateData: any[]) {
    const previewId = this.active === 'cluster' ? 'SET_TEMPLATE' : 'SERVICE_TEMPLATE';
    const { data = [] } = this.previewData.find(item => item.id === previewId) || {};
    data.forEach(item => {
      const template = templateData.find(
        template => template.bk_inst_id === item.bk_inst_id && template.bk_obj_id === item.bk_obj_id
      );
      template && this.$set(item, 'bk_inst_name', template.bk_inst_name);
    });
  }
  // 获取自定义输入表格数据
  private async getCustomInputTableData(params: any) {
    const { ipList = [] } = params;
    let data = await getHostInstanceByIp({
      ip_list: ipList.map((ip: any) => ({ ip })),
      with_external_ips: this.withExternalIps,
    }).catch(() => []);
    // 采集、策略目标选择器只能选择内网IP
    if (!this.withExternalIps) {
      data = data.filter(item => item.is_innerip);
    }
    return {
      total: data.length,
      data,
    };
  }
  // 获取动态分组表格数据
  private async getDynamicGroupTableData(params: any) {
    const result = await executeIpChooserDynamicGroup({
      scope_list: [
        {
          scope_type: 'biz',
          scope_id: this.$store.getters.bizId,
        },
      ],
      ...params,
    }).catch(() => []);
    return {
      total: result.total,
      data: result.data,
    };
  }

  // topo树数据
  private async getTopoTree() {
    const params: any = {
      instance_type: 'host',
    };
    return await getTopoTree(params).catch(() => []);
  }
  // 表格check表更事件(请勿修改selectionsData里面的数据)
  @Emit('check-change')
  private handleCheckChange(selectionsData: ITableCheckData) {
    if (this.active === 'dynamic-topo') {
      this.dynamicTableCheckChange(selectionsData);
      this.handleSetDefaultCheckedNodes();
    }
    if (['service-template', 'cluster'].includes(this.active)) {
      this.templateCheckChange(selectionsData);
    }
    if (['static-topo', 'custom-input'].includes(this.active)) {
      this.staticIpTableCheckChange(selectionsData);
    }
    if (this.active === 'dynamic-group') {
      this.dynamicGroupCheckChange(selectionsData);
    }
    return this.getCheckedData();
  }

  // 动态分组check事件
  private dynamicGroupCheckChange(selectionsData: ITableCheckData) {
    const { selections = [] } = selectionsData;
    const index = this.previewData.findIndex(item => item.id === 'DYNAMIC_GROUP');
    if (index > -1) {
      this.previewData[index].data = [...selections];
    } else {
      // 初始化分组信息(模板类型都属于动态拓扑)
      this.previewData.push({
        id: 'DYNAMIC_GROUP',
        name: this.$t('动态分组'),
        data: [...selections],
        dataNameKey: 'name',
      });
    }
  }

  // 模板类型check事件
  private templateCheckChange(selectionsData: ITableCheckData) {
    const { selections = [] } = selectionsData;
    const type = this.active === 'service-template' ? 'SERVICE_TEMPLATE' : 'SET_TEMPLATE';
    const index = this.previewData.findIndex(item => item.id === type);
    if (index > -1) {
      this.previewData[index].data = [...selections];
    } else {
      // 初始化分组信息(模板类型都属于动态拓扑)
      this.previewData.push({
        id: type,
        name: this.active === 'service-template' ? this.$t('服务模板') : this.$t('集群模板'),
        data: [...selections],
        dataNameKey: 'bk_inst_name',
      });
    }
  }
  // 动态类型表格check事件
  private dynamicTableCheckChange(selectionsData: ITableCheckData) {
    const { selections = [], excludeData = [] } = selectionsData;
    const index = this.previewData.findIndex(item => item.id === 'TOPO');
    if (index > -1) {
      const { data } = this.previewData[index];
      selections.forEach(select => {
        const index = data.findIndex(
          data => data.bk_inst_id === select.bk_inst_id && data.bk_obj_id === select.bk_obj_id
        );

        index === -1 && data.push(select);
      });
      excludeData.forEach(exclude => {
        const index = data.findIndex(
          data => data.bk_inst_id === exclude.bk_inst_id && data.bk_obj_id === exclude.bk_obj_id
        );

        index > -1 && data.splice(index, 1);
      });
    } else {
      // 初始化分组信息
      this.previewData.push({
        id: 'TOPO',
        name: this.$t('拓扑节点'),
        data: [...selections],
        dataNameKey: 'node_path',
      });
    }
  }
  private getIpKey(item: any, hasCloudId: boolean) {
    return hasCloudId ? `${item.bk_cloud_id}${item.ip}` : `${item.ip}`;
  }
  // 静态类型的表格check事件
  private staticIpTableCheckChange(selectionsData: ITableCheckData) {
    console.time();
    const { selections = [], excludeData = [] } = selectionsData;
    const index = this.previewData.findIndex(item => item.id === 'INSTANCE');
    if (index > -1) {
      const { data = [] } = this.previewData[index];
      const hasCloudId = !data[0] || Object.hasOwn(data[0], 'bk_cloud_id'); // 兼容拨测选择的IP不带云区域问题
      const dataMap = (JSON.parse(JSON.stringify(data)) as any[]).reduce<any>((pre, next, index) => {
        next.index = index;
        const key = this.getIpKey(next, hasCloudId);
        pre[key] = next;
        return pre;
      }, {});
      selections.forEach(select => {
        const key = this.getIpKey(select, hasCloudId);
        !dataMap[key] && data.push(select);
      });
      const indexes: number[] = excludeData
        .reduce((pre, next) => {
          const key = this.getIpKey(next, hasCloudId);
          const item = dataMap[key];
          if (item) {
            pre.push(item.index);
          }
          return pre;
        }, [])
        .sort((pre: number, next: number) => next - pre);

      indexes.forEach(index => {
        data.splice(index, 1);
      });
    } else {
      // 初始化分组信息
      this.previewData.push({
        id: 'INSTANCE',
        name: 'IP',
        data: [...selections],
        dataNameKey: 'ip',
      });
    }
    console.timeEnd();
  }

  // tree搜索勾选事件
  private async handleSearchSelectionChange(checkData: ITableCheckData) {
    const { selections = [], excludeData = [] } = checkData;
    this.isLoading = true;
    let data = selections;
    let exclude = excludeData;
    // if (this.active === 'dynamic-topo') {
    //   data = this.targetObjectType === 'SERVICE'
    //     ? await getServiceInstanceByNode({ node_list: selections }).catch(() => [])
    //     : await getHostInstanceByNode({ node_list: selections }).catch(() => [])
    // } else {
    //   const ipNodes = this.getNodesIpList(selections).reduce<any[]>((pre, next) => {
    //   // IP去重
    //     const index = pre.findIndex(item => this.identityIp(item, next))
    //     index === -1 && pre.push(next)
    //     return pre
    //   }, [])
    //   data = await getHostInstanceByIp({
    //     ip_list: ipNodes.map((item) => {
    //       const { ip, bk_cloud_id } = item
    //       return {
    //         ip,
    //         bk_cloud_id
    //       }
    //     })
    //   }).catch(() => [])
    // }
    if (this.active === 'static-topo') {
      // 静态拓扑勾选节点表示获取节点下面的IP
      data = this.getNodesIpList(selections).reduce<any[]>((pre, next) => {
        // IP去重
        const index = pre.findIndex(item => this.identityIp(item, next));
        index === -1 && pre.push(next);
        return pre;
      }, []);
      exclude = this.getNodesIpList(excludeData).reduce<any[]>((pre, next) => {
        // IP去重
        const index = pre.findIndex(item => this.identityIp(item, next));
        index === -1 && pre.push(next);
        return pre;
      }, []);
    } else {
      // 拿到拓扑下面的主机数量
      data =
        this.targetObjectType === 'SERVICE'
          ? await getServiceInstanceByNode({ node_list: selections }).catch(() => [])
          : await getHostInstanceByNode({ node_list: selections }).catch(() => []);
    }
    const selectionsData: ITableCheckData = {
      selections: data,
      excludeData: exclude,
    };
    this.handleCheckChange(selectionsData);
    this.isLoading = false;
  }

  // 移除节点
  @Emit('check-change')
  private handleRemoveNode({ child, item }: { child: any; item: IPreviewData }) {
    const group = this.previewData.find(data => data.id === item.id);
    if (group) {
      const index = group.data.findIndex(data => {
        if (group.id === 'TOPO') {
          return data.bk_inst_id === child.bk_inst_id && data.bk_obj_id === child.bk_obj_id;
        }
        if (group.id === 'INSTANCE') {
          return this.identityIp(data, child);
        }
        if (group.id === 'DYNAMIC_GROUP') {
          return data.id === child.id;
        }
        return data.bk_inst_id === child.bk_inst_id;
      });
      index > -1 && group.data.splice(index, 1);
    }
    this.selector.handleGetDefaultSelections();
    item.id === 'TOPO' && this.handleSetDefaultCheckedNodes();
    return this.getCheckedData();
  }
  // 当前表格check项
  private getDefaultSelections(row: any) {
    if (this.active === 'dynamic-topo') {
      const group = this.previewData.find(data => data.id === 'TOPO');
      return group?.data.some(data => data.bk_inst_id === row.bk_inst_id && data.bk_obj_id === row.bk_obj_id);
    }
    if (['static-topo', 'custom-input'].includes(this.active)) {
      const group = this.previewData.find(data => data.id === 'INSTANCE');
      return group?.data.some(data => this.identityIp(data, row));
    }
    if (['service-template', 'cluster'].includes(this.active)) {
      const type = this.active === 'service-template' ? 'SERVICE_TEMPLATE' : 'SET_TEMPLATE';
      const group = this.previewData.find(data => data.id === type);
      return group?.data.some(data => data.bk_inst_id === row.bk_inst_id);
    }
    if (['dynamic-group'].includes(this.active)) {
      const group = this.previewData.find(data => data.id === 'DYNAMIC_GROUP');
      return group?.data.some(data => data.id === row.id);
    }
    return false;
  }
  // 获取当前拓扑树搜索面板的默认勾选项
  private getSearchResultSelections(data: any) {
    const group = this.previewData.find(data => data.id === 'TOPO');
    return group?.data.some(item => data.bk_inst_id === item.bk_inst_id && data.bk_obj_id === item.bk_obj_id);
  }
  // 预览菜单点击事件
  private handleMenuClick({ menu, item }: { item: IPreviewData; menu: IMenu }) {
    if (menu.id === 'removeAll') {
      //   const group = this.previewData.find(data => data.id === item.id)
      //   group && (group.data = [])
      const index = this.previewData.findIndex(data => data.id === item.id);
      index > -1 && this.previewData.splice(index, 1);
      this.selector.handleGetDefaultSelections();
      item.id === 'TOPO' && this.handleSetDefaultCheckedNodes();
      this.$emit('check-change', this.getCheckedData());
    } else if (menu.id === 'copyIp') {
      const { data = [] } = this.previewData.find(data => data.id === item.id);
      const ipList = data.map(item => item.ip).join('\n');
      copyText(ipList);
      this.$bkMessage({
        theme: 'success',
        message: this.$t('成功复制{0}个IP', [ipList.split('\n').length]),
      });
    }
  }
  // 获取勾选的数据
  private getCheckedData() {
    // 默认只能选择一种方式
    const previewData = this.previewData.filter(item => item?.data?.length);
    const [group] = previewData;
    if (previewData.length !== 1 || group.data.length === 0) return { type: this.currentTargetNodeType, data: [] };
    return {
      type: group.id,
      data: group.data,
    };
  }
  // 判断IP是否一样
  private identityIp(pre: any, next: any) {
    // 有云区域id时，云区域ID加IP唯一标识一个IP，没有云区域时IP作为唯一ID
    return pre.bk_cloud_id !== undefined
      ? pre.ip === next.ip && pre.bk_cloud_id === next.bk_cloud_id
      : pre.ip === next.ip;
  }
  private resize() {
    this.$nextTick(() => {
      this.selectorKey = Math.random() * 10;
    });
  }
}
</script>
<style lang="scss" scoped>
.ip-selector {
  min-width: 600px;
}
</style>
