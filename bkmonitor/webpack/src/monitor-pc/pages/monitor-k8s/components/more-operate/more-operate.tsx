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
import { Component, Emit, Prop, Ref } from 'vue-property-decorator';
import { Component as tsc } from 'vue-tsx-support';

import type { ILinkItem } from '../../monitor-k8s/typings/table';

import './more-operate.scss';

interface IEvents {
  onOptionClick?: ILinkItem;
}

interface IProps {
  options?: ILinkItem[];
}

@Component
export default class MoreOperate extends tsc<IProps, IEvents> {
  @Prop({ type: Array, default: () => [] }) options: ILinkItem[];
  @Ref('moreItems') moreItemsRef: HTMLDivElement;

  popoverInstance = null;

  @Emit('optionClick')
  handleOptionClick(value: ILinkItem, e: MouseEvent) {
    e.stopPropagation?.();
    return value;
  }

  handleShowPopover(e: Event) {
    if (!this.popoverInstance) {
      this.popoverInstance = this.$bkPopover(e.target, {
        content: this.moreItemsRef,
        arrow: false,
        trigger: 'click',
        placement: 'bottom-start',
        theme: 'light common-monitor',
        boundary: 'window',
        maxWidth: 520,
        duration: [200, 0],
        onHidden: () => {
          this.popoverInstance.destroy();
          this.popoverInstance = null;
        },
      });
    }
    this.popoverInstance?.show(100);
  }

  render() {
    return (
      <div class='table-more-operate-component'>
        {this.options?.length ? (
          <div
            class='option-more'
            onClick={this.handleShowPopover}
          >
            <span class='bk-icon icon-more' />
          </div>
        ) : undefined}
        <div style={{ display: 'none' }}>
          <div
            ref='moreItems'
            class='table-more-operate-component-more-items'
          >
            {this.options.map(item => (
              <span
                class='more-item'
                onClick={e => this.handleOptionClick(item, e)}
              >
                {item.value}
              </span>
            ))}
          </div>
        </div>
      </div>
    );
  }
}
