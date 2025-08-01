/*
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
 */

import { Component, Prop, Emit, Watch } from 'vue-property-decorator';
import { Component as tsc } from 'vue-tsx-support';

import { blobDownload } from '@/common/util';

import { BK_LOG_STORAGE } from '../../../store/store.type';
import AggChart from './agg-chart';
import FieldAnalysis from './field-analysis';
import { axiosInstance } from '@/api';

import './field-item.scss';
@Component
export default class FieldItem extends tsc<object> {
  @Prop({ type: String, default: 'visible', validator: v => ['visible', 'hidden'].includes(v as string) }) type: string;
  @Prop({ type: Object, default: () => ({}) }) fieldItem: any;
  @Prop({ type: Object, default: () => ({}) }) fieldAliasMap: object;
  @Prop({ type: Boolean, default: false }) showFieldAlias: boolean;
  @Prop({ type: Array, default: () => [] }) datePickerValue: Array<any>;
  @Prop({ type: Number, default: 0 }) retrieveSearchNumber: number;
  @Prop({ type: Object, required: true }) retrieveParams: object;
  @Prop({ type: Array, default: () => [] }) visibleFields: Array<any>;
  @Prop({ type: Object, default: () => ({}) }) statisticalFieldData: object;
  @Prop({ type: Boolean, required: true }) isFrontStatistics: boolean;
  @Prop({ type: Boolean, default: false }) isFieldObject: boolean;

  isExpand = false;
  analysisActive = false;
  operationInstance = null;
  fieldAnalysisInstance = null;
  ifShowMore = false;
  fieldData = null;
  distinctCount = 0;
  btnLoading = false;
  expandIconShow = false;
  get fieldTypeMap() {
    return this.$store.state.globals.fieldTypeMap;
  }
  get unionIndexList() {
    return this.$store.getters.unionIndexList;
  }
  get isUnionSearch() {
    return this.$store.getters.isUnionSearch;
  }
  get unionIndexItemList() {
    return this.$store.getters.unionIndexItemList;
  }
  get indexSetList() {
    return this.$store.state.retrieve?.indexSetList ?? [];
  }
  get gatherFieldsCount() {
    if (this.isFrontStatistics) return Object.keys(this.statisticalFieldData).length;
    return 0;
  }
  // 显示融合字段统计比例图表
  get showFieldsChart() {
    if (this.fieldItem.field_type === 'text') return false;
    return this.isFrontStatistics ? !!this.gatherFieldsCount : this.isShowFieldsAnalysis;
  }
  get isShowFieldsCount() {
    return !['object', 'nested', 'text'].includes(this.fieldItem.field_type) && this.isFrontStatistics;
  }
  get isShowFieldsAnalysis() {
    return (
      ['keyword', 'integer', 'long', 'double', 'bool', 'conflict'].includes(this.fieldItem.field_type) &&
      this.fieldItem.es_doc_values &&
      !/^__dist_/.test(this.fieldItem.field_name)
    );
  }
  /** 冲突字段索引集名称*/
  get unionConflictFieldsName() {
    return this.unionIndexItemList
      .filter(item => this.unionIndexList.includes(item.index_set_id))
      .map(item => item.indexName);
  }

  beforeDestroy() {
    this.instanceDestroy();
  }
  // 数据变化后关闭图表分析
  @Watch('statisticalFieldData')
  statisticalFieldDataChange() {
    this.instanceDestroy();
  }
  @Emit('toggleItem')
  emitToggleItem(v) {
    return v;
  }

  getFieldIcon(fieldType: string) {
    return this.fieldTypeMap[fieldType] ? this.fieldTypeMap[fieldType].icon : 'bklog-icon bklog-unkown';
  }

  // 显示或隐藏字段
  handleShowOrHiddenItem() {
    this.instanceDestroy();
    this.emitToggleItem({
      type: this.type,
      fieldItem: this.fieldItem,
    });
  }
  showMore(fieldData, show: boolean) {
    this.ifShowMore = show;
    this.fieldData = fieldData;
  }
  closeSlider() {
    this.ifShowMore = false;
  }
  handleClickAnalysisItem() {
    this.instanceDestroy();
    this.analysisActive = true;
    this.fieldAnalysisInstance = new FieldAnalysis({
      router: this.$router,
      store: this.$store,
    });
    const indexSetIDs = this.isUnionSearch
      ? this.unionIndexList
      : [window.__IS_MONITOR_COMPONENT__ ? this.$route.query.indexId : this.$route.params.indexId];
    this.fieldAnalysisInstance.$props.queryParams = {
      ...this.retrieveParams,
      index_set_ids: indexSetIDs,
      field_type: this.fieldItem.field_type,
      agg_field: this.fieldItem.field_name,
      statisticalFieldData: this.statisticalFieldData,
      isFrontStatisticsL: this.isFrontStatistics,
    };
    this.fieldAnalysisInstance.$mount();
    /** 当小窗位置过于靠近底部时会显示不全chart图表，需要等接口更新完后更新Popper位置 */
    this.fieldAnalysisInstance?.$on('statisticsInfoFinish', this.updatePopperInstance);
    /** 字段下载功能 */
    this.fieldAnalysisInstance?.$on('downloadFieldStatistics', this.downloadFieldStatistics);
    this.fieldAnalysisInstance?.$on('showMore', this.showMore);
    this.operationInstance = this.$bkPopover(this.$refs.operationRef, {
      content: this.fieldAnalysisInstance.$el,
      arrow: true,
      placement: 'right-start',
      boundary: 'viewport',
      trigger: 'click',
      theme: 'light analysis-chart',
      interactive: true,
      appendTo: document.body,
      onHidden: () => {
        this.instanceDestroy();
      },
    });
    this.operationInstance.show(100);
  }
  /** 更新Popper位置 */
  updatePopperInstance() {
    setTimeout(() => {
      this.operationInstance.popperInstance.update();
    }, 100);
  }
  instanceDestroy() {
    this.fieldAnalysisInstance?.$off('statisticsInfoFinish', this.updatePopperInstance);
    this.fieldAnalysisInstance?.$off('downloadFieldStatistics', this.downloadFieldStatistics);
    this.operationInstance?.destroy();
    this.fieldAnalysisInstance?.$destroy();
    this.operationInstance = null;
    this.fieldAnalysisInstance = null;
    this.analysisActive = false;
  }
  /** 联合查询并且有冲突字段 */
  isUnionConflictFields(fieldType: string) {
    return this.isUnionSearch && fieldType === 'conflict';
  }

  getFieldIconColor = type => {
    return this.fieldTypeMap?.[type] ? this.fieldTypeMap?.[type]?.color : '#EAEBF0';
  };

  getFieldIconTextColor = type => {
    return this.fieldTypeMap?.[type]?.textColor;
  };
  downloadFieldStatistics() {
    this.btnLoading = true;
    const indexSetIDs = this.isUnionSearch
      ? this.unionIndexList
      : [window.__IS_MONITOR_COMPONENT__ ? this.$route.query.indexId : this.$route.params.indexId];
    const downRequestUrl = '/field/index_set/fetch_value_list/';
    const data = {
      ...this.retrieveParams,
      index_set_ids: indexSetIDs,
      field_type: this.fieldItem.field_type,
      agg_field: this.fieldItem.field_name,
      limit: this.fieldData?.distinct_count,
    };
    axiosInstance
      .post(downRequestUrl, data)
      .then(res => {
        if (typeof res !== 'string') {
          this.$bkMessage({
            theme: 'error',
            message: this.$t('下载失败'),
          });
          return;
        }
        const routerIndexSet = window.__IS_MONITOR_COMPONENT__ ? this.$route.query.indexId : this.$route.params.indexId;
        const lightName = this.indexSetList.find(item => item.index_set_id === routerIndexSet)?.lightenName;
        const downloadName = `bk_log_search__${lightName.substring(2, lightName.length - 1)}_${this.fieldItem.field_name}.csv`;
        blobDownload(res, downloadName);
      })
      .finally(() => {
        this.btnLoading = false;
      });
  }
  getdistinctCount(val) {
    this.distinctCount = val;
  }
  retuanFieldName() {
    let name = this.$store.state.storage[BK_LOG_STORAGE.SHOW_FIELD_ALIAS]
      ? this.fieldItem.query_alias || this.fieldItem.alias_name || this.fieldItem.field_name
      : this.fieldItem.field_name;
    if (this.isFieldObject) {
      const objectName = name.split('.');
      name = objectName[objectName.length - 1] || objectName[0];
    }
    return name;
  }
  render() {
    return (
      <li class='filed-item'>
        <div class={{ 'filed-title': true, expanded: this.isExpand }}>
          <div
            onClick={() => {
              // 联合查询 或 非白名单业务和索引集类型 时不能点击弹出字段分析
              if (!this.isShowFieldsAnalysis || this.isUnionSearch || this.isFrontStatistics) return;
              this.handleClickAnalysisItem();
            }}
          >
            {/* 拖动字段位置按钮 */}
            <div class='bklog-drag-dots-box'>
              <span class={['icon bklog-icon bklog-drag-dots', { 'hidden-icon': this.type === 'hidden' }]} />
            </div>

            {/* 字段类型对应的图标 */}
            <div class='bklog-field-icon'>
              <span
                style={{
                  backgroundColor: this.fieldItem.is_full_text
                    ? false
                    : this.getFieldIconColor(this.fieldItem.field_type),
                  color: this.fieldItem.is_full_text ? false : this.getFieldIconTextColor(this.fieldItem.field_type),
                }}
                class={[this.getFieldIcon(this.fieldItem.field_type) || 'bklog-icon bklog-unkown', 'field-type-icon']}
                v-bk-tooltips={{
                  content: this.fieldTypeMap[this.fieldItem.field_type]?.name,
                  disabled: !this.fieldTypeMap[this.fieldItem.field_type],
                }}
              />
            </div>

            {/* 字段名 */}
            <span>
              <span class='field-name'>{this.retuanFieldName()}</span>
              {this.fieldItem.children?.length && <span class='field-badge'>{this.fieldItem.children?.length}</span>}
              {this.fieldItem.children?.length &&
                (this.expandIconShow ? (
                  <span class={['bk-icon', 'icon-angle-up', 'expand']}></span>
                ) : (
                  <span class={['bk-icon', 'icon-angle-down', 'expand']}></span>
                ))}

              {this.isUnionConflictFields(this.fieldItem.field_type) && (
                <bk-popover
                  ext-cls='conflict-popover'
                  theme='light'
                >
                  <i class='conflict-icon bk-icon icon-exclamation-triangle-shape' />
                  <div slot='content'>
                    <p>{this.$t('该字段在以下索引集存在冲突')}</p>
                    {this.unionConflictFieldsName.map(item => (
                      <bk-tag>{item}</bk-tag>
                    ))}
                  </div>
                </bk-popover>
              )}
            </span>
          </div>
          <div
            ref='operationRef'
            class={['operation-text', { 'analysis-active': this.analysisActive }]}
          >
            {this.isShowFieldsAnalysis && (
              <div
                class={{
                  'operation-icon-box': true,
                  'analysis-disabled': this.isUnionSearch || this.isFrontStatistics,
                }}
                v-bk-tooltips={{
                  content: this.isUnionSearch || this.isFrontStatistics ? this.$t('暂不支持') : this.$t('图表分析'),
                }}
                onClick={e => {
                  e.stopPropagation();
                  // 联合查询 或 非白名单业务和索引集类型 时不能点击字段分析
                  if (this.isUnionSearch || this.isFrontStatistics) return;
                  this.handleClickAnalysisItem();
                }}
              >
                <i class='bklog-icon bklog-chart-2' />
              </div>
            )}
            {/* 设置字段显示或隐藏 */}
            {
              <div
                class='operation-icon-box'
                v-bk-tooltips={{
                  content: this.type === 'visible' ? this.$t('隐藏') : this.$t('显示'),
                }}
                onClick={e => {
                  e.stopPropagation();
                  this.handleShowOrHiddenItem();
                }}
              >
                <i class={['bk-icon include-icon', `${this.type === 'visible' ? 'icon-eye' : 'icon-eye-slash'}`]}></i>
              </div>
            }
          </div>
        </div>

        <bk-sideslider
          width={480}
          class='agg-field-item-sideslider'
          is-show={this.ifShowMore}
          quick-close={true}
          show-mask={false}
          transfer
          onAnimation-end={this.closeSlider}
        >
          <template slot='header'>
            <div class='agg-sides-slider-header'>
              <div class='distinct-num'>
                <span class='field-name'>{this.fieldItem?.field_name}</span>
                <div class='col-line' />
                <span class='distinct-count-label'>{this.$t('去重后字段统计')}</span>
                <span class='distinct-count-num'>{`(${this.distinctCount})`}</span>
              </div>
              <div class='fnBtn'>
                <bk-button
                  loading={this.btnLoading}
                  size='small'
                  text={true}
                  onClick={e => {
                    e.stopPropagation();
                    this.downloadFieldStatistics();
                  }}
                >
                  <div class='download-btn'>
                    <i class='bklog-icon bklog-download' />
                    <span>{this.$t('下载')}</span>
                  </div>
                </bk-button>
                {/* <bk-button size='small'>查看仪表盘</bk-button> */}
              </div>
            </div>
          </template>
          <template slot='content'>
            <div class='agg-sides-content slider-content'>
              <AggChart
                field-name={this.fieldItem.field_name}
                field-type={this.fieldItem.field_type}
                is-front-statistics={this.isFrontStatistics}
                limit={this.fieldData?.distinct_count}
                parent-expand={this.isExpand}
                retrieve-params={this.retrieveParams}
                statistical-field-data={this.statisticalFieldData}
                onDistinctCount={val => this.getdistinctCount(val)}
              />
            </div>
          </template>
        </bk-sideslider>
      </li>
    );
  }
}
