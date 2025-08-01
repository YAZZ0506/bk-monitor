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
  <bk-dialog
    width="1105"
    :show-footer="false"
    :value="show"
    @value-change="handleValueChange"
  >
    <template>
      <div
        class="log-version"
        v-bkloading="{ isLoading: loading }"
      >
        <div class="log-version-left">
          <ul class="left-list">
            <li
              v-for="(item, index) in logList"
              class="left-list-item"
              :class="{ 'item-active': index === active }"
              :key="index"
              @click="handleItemClick(index)"
            >
              <span class="item-title">{{ item.title }}</span>
              <span class="item-date">{{ item.date }}</span>
              <span
                v-if="index === current"
                class="item-current"
                >{{ $t('当前版本') }}</span
              >
            </li>
          </ul>
        </div>
        <div class="log-version-right">
          <!-- eslint-disable vue/no-v-html -->
          <div
            class="detail-container"
            v-html="$xss(currentLog.detail)"
          ></div>
          <!--eslint-enable-->
        </div>
      </div>
    </template>
  </bk-dialog>
</template>

<script>
  import { axiosInstance } from '@/api';
  import { mapState } from 'vuex';

  export default {
    name: 'LogVersion',
    props: {
      // 是否显示
      dialogShow: Boolean,
    },
    data() {
      return {
        show: false,
        current: 0,
        active: 0,
        logList: [],
        loading: false,
      };
    },
    computed: {
      ...mapState({
        spaceUid: state => state.spaceUid,
        isExternal: state => state.isExternal,
      }),
      currentLog() {
        return this.logList[this.active] || {};
      },
    },
    watch: {
      dialogShow: {
        async handler(v) {
          this.show = v;
          if (v) {
            this.loading = true;
            this.logList = await this.getVersionLogsList();
            if (this.logList.length) {
              await this.handleItemClick();
            }
            this.loading = false;
          }
        },
        immediate: true,
      },
    },
    beforeUnmount() {
      this.show = false;
      this.$emit('update:dialog-show', false);
    },
    methods: {
      //  dialog显示变更触发
      handleValueChange(v) {
        this.$emit('update:dialog-show', v);
      },
      // 点击左侧log查看详情
      async handleItemClick(v = 0) {
        this.active = v;
        if (!this.currentLog.detail) {
          this.loading = true;
          const detail = await this.getVersionLogsDetail();
          this.currentLog.detail = detail;
          this.loading = false;
        }
      },
      // 获取左侧版本日志列表
      async getVersionLogsList() {
        const params = {
          method: 'get',
          url: `${window.SITE_URL}version_log/version_logs_list/`,
        };
        if (this.isExternal) {
          params.headers = {
            'X-Bk-Space-Uid': this.spaceUid,
          };
        }
        const { data } = await axiosInstance(params).catch(_ => {
          console.warn(_);
          return { data: { data: [] } };
        });
        return data.data.map(item => ({ title: item[0], date: item[1], detail: '' }));
      },
      // 获取右侧对应的版本详情
      async getVersionLogsDetail() {
        const params = {
          method: 'get',
          url: `${window.SITE_URL}version_log/version_log_detail/`,
          params: {
            log_version: this.currentLog.title,
          },
        };
        if (this.isExternal) {
          params.headers = {
            'X-Bk-Space-Uid': this.spaceUid,
          };
        }
        const { data } = await axiosInstance(params).catch(_ => {
          console.warn(_);
          return { data: '' };
        });
        return data.data;
      },
    },
  };
</script>

<style lang="scss" scoped>
  .log-version {
    display: flex;
    margin: -33px -24px -26px;

    &-left {
      display: flex;
      flex: 0 0 180px;
      padding: 40px 0;
      font-size: 12px;
      background-color: #fafbfd;
      border-right: 1px solid #dcdee5;

      .left-list {
        display: flex;
        flex-direction: column;
        width: 100%;
        height: 520px;
        overflow: auto;
        border-top: 1px solid #dcdee5;
        border-bottom: 1px solid #dcdee5;

        &-item {
          position: relative;
          display: flex;
          flex: 0 0 54px;
          flex-direction: column;
          justify-content: center;
          padding-left: 30px;
          border-bottom: 1px solid #dcdee5;

          &:hover {
            cursor: pointer;
            background-color: #fff;
          }

          .item-title {
            font-size: 16px;
            color: #313238;
          }

          .item-date {
            color: #979ba5;
          }

          .item-current {
            position: absolute;
            top: 8px;
            right: 20px;
            display: flex;
            align-items: center;
            justify-content: center;
            width: 58px;
            height: 20px;
            color: #fff;
            background-color: #699df4;
            border-radius: 2px;
          }

          &.item-active {
            background-color: #fff;

            &::before {
              position: absolute;
              top: 0px;
              bottom: 0px;
              left: 0;
              width: 6px;
              content: ' ';
              background-color: #3a84ff;
            }
          }
        }
      }
    }

    &-right {
      flex: 1;
      padding: 25px 30px 50px 45px;

      .detail-container {
        max-height: 525px;
        overflow: auto;
      }
    }
  }
</style>
