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
import { Component, Emit, Prop, Ref, Watch } from 'vue-property-decorator';
import { Component as tsc } from 'vue-tsx-support';

import { random } from 'monitor-common/utils/utils';

import loadingIcon from '../../../icons/spinner.svg';
import { type IQueryParams, type TimeSeriesType, PanelModel } from '../../../typings';
import TimeSeries from '../../time-series/time-series';
import TraceChart from '../trace-chart/trace-chart';

import './trend-chart.scss';

const DEFAULT_PANEL_CONFIG = {
  title: '',
  gridPos: {
    x: 16,
    y: 16,
    w: 8,
    h: 4,
  },
  type: 'graph',
  targets: [],
};

interface ITrendChartEvents {
  (series: any[]): void;
  onLoonSeriesDataading(loading: boolean): void;
  onOptionsLoaded(): void;
}

interface ITrendChartProps {
  diffDate: number[][];
  queryParams: IQueryParams;
}

@Component
export default class TrendChart extends tsc<ITrendChartProps, ITrendChartEvents> {
  @Prop({ default: () => ({}), type: Object }) queryParams: IQueryParams;
  @Prop({ default: () => [], type: Array }) diffDate: number[][];
  @Ref() chartRef;

  loading = false;
  chartType = 'all';
  collapse = 'trend';
  panel: PanelModel = null;

  @Watch('chartType')
  handleChartTypeChange() {
    this.handleQueryParamsChange();
  }

  @Watch('queryParams', { deep: true })
  handleQueryParamsChange() {
    let type: TimeSeriesType = 'bar';
    let targetApi;
    let targetData;
    if (this.chartType === 'all') {
      type = 'line';
      targetApi = 'apm_profile.query';
      targetData = {
        ...this.queryParams,
        diagram_types: ['tendency'],
      };
    } else {
      type = 'bar';
      targetApi = 'apm_profile.queryProfileBarGraph';
      targetData = {
        ...this.queryParams,
      };
    }

    this.panel = new PanelModel({
      ...DEFAULT_PANEL_CONFIG,
      id: random(6),
      options: { time_series: { type } },
      targets: [
        {
          api: targetApi,
          datasource: 'time_series',
          alias: '',
          data: targetData,
        },
      ],
    });
  }

  @Watch('diffDate')
  handleDiffDateChange() {
    this.handleSetMarkArea();
  }

  handleSetMarkArea() {
    if (!this.chartRef.options) return;
    const { series, ...params } = this.chartRef.options;
    this.chartRef.setOptions({
      ...params,
      series: series.map((item, ind) => ({
        ...item,
        markArea: {
          show: !!this.diffDate[ind]?.length,
          itemStyle: {
            color: ['rgba(58, 132, 255, 0.1)', 'rgba(255, 86, 86, 0.1)'][ind],
          },
          data: [
            [
              {
                xAxis: this.diffDate[ind]?.[0] || 0,
              },
              {
                xAxis: this.diffDate[ind]?.[1] || 0,
              },
            ],
          ],
        },
      })),
    });
  }

  @Emit('optionsLoaded')
  handleOptionsLoaded() {}

  @Emit('seriesData')
  handleSeriesData(data) {
    return data.series || [];
  }

  @Emit('loading')
  handleLoading(val) {
    this.loading = val;
  }

  render() {
    const chartHtml =
      this.chartType === 'all' ? (
        <TimeSeries
          ref='chartRef'
          panel={this.panel}
          showChartHeader={false}
          showHeaderMoreTool={false}
          onLoading={this.handleLoading}
          onOptionsLoaded={this.handleOptionsLoaded}
          onSeriesData={this.handleSeriesData}
        />
      ) : (
        <TraceChart
          ref='chartRef'
          panel={this.panel}
          showChartHeader={false}
          showHeaderMoreTool={false}
          onLoading={this.handleLoading}
          onOptionsLoaded={this.handleOptionsLoaded}
          onSeriesData={this.handleSeriesData}
        />
      );

    return (
      <div class='trend-chart'>
        <bk-collapse v-model={this.collapse}>
          <bk-collapse-item
            ext-cls='trend-chart-collapse'
            scopedSlots={{
              content: () => <div class='trend-chart-wrap'>{this.collapse && this.panel && chartHtml}</div>,
            }}
            name='trend'
          >
            <div
              class='trend-chart-header'
              onClick={e => e.stopPropagation()}
            >
              <div class='bk-button-group'>
                <bk-button
                  class={`${this.chartType === 'all' ? 'is-selected' : ''}`}
                  size='small'
                  onClick={() => (this.chartType = 'all')}
                >
                  {this.$t('总趋势')}
                </bk-button>
                <bk-button
                  class={`${this.chartType === 'trace' ? 'is-selected' : ''}`}
                  size='small'
                  onClick={() => (this.chartType = 'trace')}
                >
                  {this.$t('Trace 数据')}
                </bk-button>
              </div>
              {this.loading ? (
                <img
                  class='chart-loading-icon'
                  alt='loading'
                  src={loadingIcon}
                />
              ) : undefined}
            </div>
          </bk-collapse-item>
        </bk-collapse>
      </div>
    );
  }
}
