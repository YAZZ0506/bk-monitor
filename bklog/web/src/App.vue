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
  <div
    id="app"
    v-bkloading="{ isLoading: pageLoading }"
    :class="{ 'clear-min-height': $route.name === 'retrieve' }"
    :style="noticeComponentStyle"
  >
    <NoticeComponent
      v-if="!isAsIframe"
      api-url="/notice/announcements/"
      ref="refNoticeComponent"
      @show-alert-change="showAlertChange"
    />
    <head-nav
      v-show="!isAsIframe && !pageLoading"
      :welcome-data="welcomePageData"
      @reload-router="routerKey += 1"
      @welcome="welcomePageData = $event"
    />
    <div :class="['log-search-container', isAsIframe && 'as-iframe', { 'is-show-notice': showAlert }]">
      <welcome-page
        v-if="welcomePageData"
        :data="welcomePageData"
      />
      <!-- 导航改版 -->
      <bk-navigation
        v-else-if="menuList && menuList.length"
        class="bk-log-navigation"
        :theme-color="navThemeColor"
        head-height="0"
        header-title=""
        navigation-type="left-right"
        default-open
        @toggle="handleToggle"
      >
        <template #menu>
          <!-- <div class="biz-menu">
            <biz-menu-select :is-expand="isExpand" />
          </div> -->
          <bk-navigation-menu
            :default-active="activeManageNav.id"
            :item-default-bg-color="navThemeColor"
          >
            <template v-for="groupItem in menuList">
              <bk-navigation-menu-group
                v-if="groupItem.children.length"
                :group-name="isExpand ? groupItem.name : groupItem.keyword"
                :key="groupItem.id"
              >
                <template>
                  <a
                    v-for="navItem in getGroupChildren(groupItem.children)"
                    class="nav-item"
                    :href="getRouteHref(navItem.id)"
                    :key="navItem.id"
                  >
                    <bk-navigation-menu-item
                      :data-test-id="`navBox_nav_${navItem.id}`"
                      :icon="getMenuIcon(navItem)"
                      :id="navItem.id"
                      @click="handleClickNavItem(navItem.id)"
                    >
                      <span>{{ isExpand ? navItem.name : '' }}</span>
                    </bk-navigation-menu-item>
                  </a>
                </template>
              </bk-navigation-menu-group>
            </template>
          </bk-navigation-menu>
        </template>
        <div
          v-if="!pageLoading"
          class="navigation-content"
        >
          <auth-container-page
            v-if="authPageInfo"
            :info="authPageInfo"
          ></auth-container-page>
          <router-view
            v-else
            class="manage-content"
            :key="routerKey"
          ></router-view>
        </div>
      </bk-navigation>
      <!-- 无侧边栏页面 -->
      <router-view
        v-else-if="!pageLoading && !menuList"
        class="manage-content"
        :key="routerKey"
      ></router-view>
      <novice-guide
        v-if="displayRetrieve"
        :data="guideStep"
        guide-page="default"
      />
    </div>
    <auth-dialog />
    <global-setting-dialog
      v-model="isShowGlobalDialog"
      :active-menu="globalActiveLabel"
      :menu-list="globalSettingList"
      @menu-change="handleChangeMenu"
    />
  </div>
</template>

<script>
  import BizMenuSelect from '@/global/bk-space-choice'
  import AuthContainerPage from '@/components/common/auth-container-page';
  import AuthDialog from '@/components/common/auth-dialog';
  import WelcomePage from '@/components/common/welcome-page';
  import GlobalSettingDialog from '@/components/global-setting';
  import headNav from '@/components/nav/head-nav';
  import NoviceGuide from '@/components/novice-guide';
  import { handleTransformToTimestamp } from '@/components/time-range/utils';
  import platformConfigStore from '@/store/modules/platform-config';
  import NoticeComponent from '@blueking/notice-component-vue2';
  import jsCookie from 'js-cookie';
  import { mapState, mapGetters } from 'vuex';

  import '@blueking/notice-component-vue2/dist/style.css';

  export default {
    name: 'App',
    components: {
      headNav,
      AuthContainerPage,
      AuthDialog,
      WelcomePage,
      BizMenuSelect,
      NoviceGuide,
      GlobalSettingDialog,
      NoticeComponent,
    },
    data() {
      return {
        loginData: null,
        welcomePageData: null,
        routerKey: 0,
        navThemeColor: '#2c354d',
        isExpand: true,
        curGuideStep: 0,
        rightClickRouteName: '', // 当前右键选中的路由
        visible: false, // 是否展示右键菜单
        top: 0, // 右键菜单定位top
        left: 0, // 右键菜单定位left
        /** 全局设置列表 */
        dialogSettingList: [{ id: 'masking-setting', name: this.$t('全局脱敏') }],
        noticeComponentHeight: 0,
      };
    },
    computed: {
      ...mapState([
        'topMenu',
        'activeTopMenu',
        'activeManageNav',
        'userGuideData',
        'isExternal',
        'isShowGlobalDialog',
        'globalSettingList',
        'globalActiveLabel',
        'showAlert',
      ]),
      ...mapGetters({
        pageLoading: 'pageLoading',
        isAsIframe: 'asIframe',
        authPageInfo: 'globals/authContainerInfo',
        maskingToggle: 'maskingToggle',
      }),
      navActive() {
        return '';
      },
      menuList() {
        const list = this.topMenu.find(item => item.id === this.activeTopMenu.id)?.children;
        if (this.isExternal && this.activeTopMenu.id === 'manage') {
          // 外部版只保留【日志提取】菜单
          return list.filter(menu => menu.id === 'manage-extract-strategy');
        }
        return list;
      },
      displayRetrieve() {
        return this.$store.state.retrieve.displayRetrieve;
      },
      guideStep() {
        return this.userGuideData?.default || {};
      },
      noticeComponentStyle() {
        return {
          '--notice-component-height': `${this.noticeComponentHeight}px`,
        };
      },
    },
    watch: {
      maskingToggle: {
        deep: true,
        handler(val) {
          // 更新全局操作列表
          const isShowSettingList = val.toggleString !== 'off';
          this.$store.commit('updateGlobalSettingList', isShowSettingList ? this.dialogSettingList : []);
        },
      },
    },
    created() {
      platformConfigStore.fetchConfig();
      const platform = window.navigator.platform.toLowerCase();
      if (platform.indexOf('win') === 0) {
        document.body.style['font-family'] = 'Microsoft Yahei, pingFang-SC-Regular, Helvetica, Aria, sans-serif';
      } else {
        document.body.style['font-family'] = 'pingFang-SC-Regular, Microsoft Yahei, Helvetica, Aria, sans-serif';
      }
      this.$store.commit('updateRunVersion', window.RUN_VER || '');

      // 是否转换日期类型字段格式
      const isFormatDate = jsCookie.get('operation');
      if (isFormatDate === 'false') {
        this.$store.commit('updateIsFormatDate', false);
      }
      const isEnLanguage = (jsCookie.get('blueking_language') || 'zh-cn') === 'en';
      this.$store.commit('updateIsEnLanguage', isEnLanguage);
      if(isEnLanguage){
        document.body.classList.add('language-en');
      }else {
        document.body.classList.remove('language-en');
      }
      // 初始化脱敏灰度相关的代码
      this.initMaskingToggle();

      if (!this.isAsIframe) this.getUserGuide();

      const defaultTime = localStorage.getItem('SEARCH_DEFAULT_TIME');
      if (defaultTime) {
        const datePickerValue = JSON.parse(defaultTime);
        const timeList = handleTransformToTimestamp(datePickerValue);
        this.$store.commit('updateIndexItemParams', {
          datePickerValue,
          start_time: timeList[0],
          end_time: timeList[1],
        });
      }

      this.$store.state.isExternal = window.IS_EXTERNAL ? JSON.parse(window.IS_EXTERNAL) : false;
    },

    methods: {
      /** 初始化脱敏灰度相关的数据 */
      initMaskingToggle() {
        const { log_desensitize: logDesensitize } = window.FEATURE_TOGGLE;
        let toggleList = window.FEATURE_TOGGLE_WHITE_LIST?.log_desensitize || [];
        switch (logDesensitize) {
          case 'on':
            toggleList = [];
            break;
          case 'off': {
            toggleList = [];
            // const index = this.dialogSettingList.findIndex(item => item.id === 'masking-setting');
            // const newSettingList = this.dialogSettingList.slice(index, 1);
            this.$store.commit('updateGlobalSettingList', []);
            break;
          }
          default:
            break;
        }
        this.$store.commit('updateMaskingToggle', {
          toggleString: logDesensitize,
          toggleList,
        });
      },
      /** 更新全局弹窗的选项 */
      handleChangeMenu(item) {
        this.$store.commit('updateGlobalActiveLabel', item.id);
      },
      getMenuIcon(item) {
        if (item.icon) {
          return `bk-icon bklog-icon bklog-${item.icon}`;
        }

        return 'bk-icon icon-home-shape';
      },
      handleClickNavItem(id) {
        this.$router.push({
          name: id,
          query: {
            spaceUid: this.$store.state.spaceUid,
          },
        });
        if (id === 'default-dashboard') {
          this.routerKey = this.routerKey + 1;
        }
      },
      handleToggle(val) {
        this.isExpand = val;
      },
      getUserGuide() {
        this.$http
          .request('meta/getUserGuide')
          .then(res => {
            this.$store.commit('setUserGuideData', res.data);
          })
          .catch(e => {
            console.warn(e);
          });
      },
      getRouteHref(pageName) {
        const newUrl = this.$router.resolve({
          name: pageName,
          query: {
            spaceUid: this.$store.state.spaceUid,
          },
        });
        return newUrl.href;
      },
      /** 侧边导航菜单 */
      getGroupChildren(list) {
        if (this.isExternal && this.activeTopMenu.id === 'manage') {
          // 外部版只保留【日志提取任务】
          return list.filter(menu => menu.id === 'log-extract-task');
        }
        return list;
      },
      showAlertChange(v) {
        this.$store.commit('updateNoticeAlert', v);
        const refNoticeComponent = this.$refs.refNoticeComponent;
        if (refNoticeComponent) {
          this.noticeComponentHeight = refNoticeComponent.$el.offsetHeight;
        }
      },
    },
  };
</script>

<style lang="scss">
  @import './scss/reset.scss';
  @import './scss/app.scss';
  @import './scss/animation.scss';
  @import './scss/mixins/clearfix.scss';
  @import './scss/mixins/scroller.scss';

  #app {
    min-width: 1280px;
    height: 100%;
    min-height: 730px;
    background: #f4f7fa;
  }

  .clear-min-height {
    /* stylelint-disable-next-line declaration-no-important */
    min-height: 0 !important;
  }

  .button-text {
    color: #3a84ff;
    cursor: pointer;

    &:hover {
      color: #699df4;
    }

    &:active {
      color: #2761dd;
    }

    &.is-disabled {
      color: #c4c6cc;
      cursor: not-allowed;
    }
  }

  .text-overflow-hidden {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .log-search-container {
    position: relative;
    width: 100%;
    height: calc(100% - 52px - var(--notice-component-height));
    overflow-y: hidden;

    &.as-iframe {
      height: 100%;
    }

    &.is-show-notice {
      height: calc(100% - 90px);

      .sub-nav-container {
        top: 91px;
      }

      .masking-dialog {
        .bk-dialog {
          /* stylelint-disable-next-line declaration-no-important */
          top: 92px !important;
        }
      }
    }
  }

  /*无权限时 v-cursor 样式*/
  .cursor-element {
    width: 12px;
    height: 16px;
    background: url('./images/cursor-lock.svg') no-repeat;
  }
  // 检索里一些公用的样式
  .tab-button {
    float: left;

    @include clearfix;

    .tab-button-item {
      padding: 0 15px;
      margin-left: -1px;
      font-size: 0;
      color: #63656e;
      cursor: pointer;
      border: 1px solid #c4c6cc;
      border-left-color: transparent;

      &:first-child {
        margin-left: 0;
        border-left-color: #c4c6cc;
        border-radius: 2px 0 0 2px;
      }

      &:last-child {
        border-radius: 0 2px 2px 0;
      }

      &.active {
        z-index: 10;
        color: #3a84ff;
        background: #e1ecff;
        border: 1px solid #3a84ff;
      }
    }

    .tab-button-text {
      display: inline-block;
      width: 100%;
      overflow: hidden;
      font-size: 12px;
      line-height: 32px;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
  }
  // hack 组件样式
  .bk-dialog.bk-info-box .bk-dialog-header-inner {
    /* stylelint-disable-next-line declaration-no-important */
    white-space: normal !important;
  }

  .bk-date-picker-dropdown .bk-picker-confirm-time {
    color: #3a84ff;
  }

  .tippy-tooltip .tippy-content {
    padding: 0;
    word-break: break-all;
  }

  .bk-form-control.is-error .bk-form-input {
    border-color: #ff5656;
  }
  // 导航
  .bk-log-navigation.bk-navigation,
  .hack-king-navigation.bk-navigation {
    /* stylelint-disable-next-line declaration-no-important */
    width: 100% !important;

    /* stylelint-disable-next-line declaration-no-important */
    height: 100% !important;

    .container-header {
      /* stylelint-disable-next-line declaration-no-important */
      display: none !important;
    }

    .bk-navigation-wrapper {
      height: 100%;

      .navigation-container {
        z-index: 100;

        /* stylelint-disable-next-line declaration-no-important */
        max-width: calc(100% - 60px) !important;

        .container-content {
          /* stylelint-disable-next-line declaration-no-important */
          height: 100% !important;

          /* stylelint-disable-next-line declaration-no-important */
          max-height: 100% !important;
          padding: 0;

          .navigation-content {
            height: 100%;
          }
        }
      }

      .bk-navigation-menu-group {
        .group-name-wrap .group-name {
          margin-right: 0;
        }
      }

      .navigation-menu-item-icon.bk-icon {
        min-width: 28px;
      }

      .nav-item {
        display: inline-block;
        width: 100%;
      }
    }

    .nav-slider-list {
      /* stylelint-disable-next-line declaration-no-important */
      height: calc(100% - 56px) !important;
    }
  }

  .biz-menu {
    padding-bottom: 10px;
    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
  }

  // 表格单元 v-bk-overflow-tips
  .bk-table .bk-table-body-wrapper .table-ceil-container {
    width: 100%;

    > span {
      display: block;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
  }
  // hack vue-json-pretty
  .json-view-wrapper .vjs-value {
    word-break: break-all;
  }
  // hack be-select将下拉宽度全部交给slot以控制宽度和事件传播
  .custom-no-padding-option.bk-option > .bk-option-content {
    padding: 0;

    &.is-selected {
      color: #3a84ff;
      background-color: #e1ecff;
    }

    > .option-slot-container {
      min-height: 32px;
      padding: 9px 16px;
      line-height: 14px;

      &.no-authority {
        display: flex;
        align-items: center;
        justify-content: space-between;
        color: #c4c6cc;
        cursor: not-allowed;

        .text {
          width: calc(100% - 56px);
        }

        .apply-text {
          display: none;
          flex-shrink: 0;
          color: #3a84ff;
          cursor: pointer;
        }

        &:hover .apply-text {
          display: flex;
        }
      }
    }
  }
  // 采集项管理、索引集管理通用样式
  .access-manage-container {
    padding: 28px 24px;

    .bk-tab-section {
      display: none;
    }

    .go-search {
      // position: fixed;
      position: absolute;
      right: 0;
      z-index: 999;
      font-size: 12px;

      .icon-info {
        font-size: 14px;
        color: #979ba5;
      }

      .search-button {
        display: inline-block;
        color: #3a84ff;
        cursor: pointer;
      }

      .search-text {
        height: 32px;
        padding: 0 9px;
        line-height: 32px;
        background: #fff;
        border-radius: 2px;
        box-shadow: 0 2px 4px 0 #1919290d;
      }
    }

    .tab-content {
      height: calc(100% - 50px);
      padding: 20px;
      overflow: auto;
      background-color: #fff;
      border-top: none;
      box-shadow: 0 2px 4px 0 #1919290d;

      @include scroller($backgroundColor: #c4c6cc, $width: 4px);

      .main-title {
        display: flex;
        align-items: flex-end;
        justify-content: space-between;
        padding: 0 0 8px 0;
        margin-bottom: 20px;
        font-size: 14px;
        font-weight: 700;
        line-height: 20px;
        color: #63656e;
        border-bottom: 1px solid #dcdee5;
      }

      .refresh-button {
        display: flex;
        align-items: center;
        margin-left: 8px;
        font-size: 12px;
        font-weight: normal;
        color: #3a84ff;
        cursor: pointer;

        &:hover {
          color: #699df4;
        }

        .bk-icon {
          margin-right: 4px;
          font-size: 13px;
        }
      }

      .charts-container {
        display: flex;
        justify-content: space-between;
        margin-bottom: 20px;

        .chart-container {
          position: relative;
          width: calc((100% - 16px) / 2);
          padding: 0 16px;
          border: 1px solid #f0f1f5;
          border-radius: 3px;
          box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.05);

          .chart-header {
            display: flex;
            align-items: center;
            justify-content: space-between;
            height: 50px;

            .title {
              font-size: 12px;
              font-weight: bold;
              line-height: 16px;
              color: #63656e;
            }

            .date-picker {
              display: flex;
              align-items: center;
            }
          }

          .chart-canvas-container {
            position: relative;
            height: 230px;

            &.big-chart {
              height: 280px;
            }
          }

          .king-exception {
            position: absolute;
            top: 80px;
            left: 0;
          }
        }
      }
    }
  }

  .title-overflow {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .beta-class {
    padding-top: 3px;
    margin-left: 2px;
    color: #ffa228;
  }

  .bk-dialog-type-header .header {
    /* stylelint-disable-next-line declaration-no-important */
    white-space: normal !important;
  }

  .bk-options .bk-option {
    &:hover {
      color: #63656e;
      background-color: #f5f7fa;
    }
  }

  :root {
    --table-fount-family: Menlo, Monaco, Consolas, Courier, 'PingFang SC', 'Microsoft Yahei', monospace;
    --table-fount-size: 12px;
    --table-fount-color: #16171a;
  }

  .bk-log-drag {
    position: absolute;
    top: 50%;
    left: 242px;
    z-index: 100;
    display: flex;
    align-items: center;
    justify-items: center;
    width: 6px;
    height: 18px;
    background-color: transparent;
    border-radius: 3px;
    transform: translateY(-50%);

    &::after {
      position: absolute;
      left: 2px;
      width: 0;
      height: 18px;
      content: ' ';
      border-left: 2px dotted #979ba5;
    }

    &:hover {
      cursor: col-resize;
    }
  }

  .bk-log-drag-simple {
    position: absolute;
    top: 50%;
    right: -3px;
    z-index: 100;
    display: flex;
    align-items: center;
    justify-items: center;
    width: 6px;
    height: 22px;
    border-radius: 3px;
    transform: translateY(-50%);

    &::after {
      position: absolute;
      left: 2px;
      width: 0;
      height: 100%;
      content: ' ';
      border-left: 2px dotted #63656e;
    }

    &:hover {
      cursor: col-resize;
    }
  }

  // .bk-label {
  //   .bk-label-text {
  //     font-size: 12px;
  //   }

  //   &::after {
  //     top: 54%;
  //   }
  // }
</style>
