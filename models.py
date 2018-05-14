from functools import partial

import pandas as pd
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func

from .consts import QUANSHI_
from .utils import Month, Week, gr2change


class WinsunDatabase:
    @staticmethod
    def render_date(by, period):
        """通过日期类型和期数两个参数得到一系列日期相关参数"""
        if by == 'week':
            d = Week()
            index = [d.before(i) for i in range(period)]
            index.reverse()
            index_label = [f'{x.monday_str}-{x.sunday_str}' for x in index]
            index_sql = [x.string for x in index]
            group_by = '星期'

        else:
            d = Month()
            index = [d.before(i) for i in range(period)]
            index.reverse()
            index_label = [x.string[:-2] for x in index]
            index_sql = [x.date for x in index]
            group_by = '年月'

        date_range = (index_sql[0], index_sql[-1])

        return date_range, group_by, index_sql, index_label

    def __init__(self):
        engine = create_engine('sqlite:///e:/gisDataBase/winsun')
        Session = sessionmaker(engine)
        self.session = Session()
        self.meta = MetaData()
        self.meta.reflect(bind=engine)

    def __getitem__(self, item):
        """通过 wd['table_name'] 的方式选中表"""
        return self.meta.tables[item]

    def query(self, table):
        """查询指定表，可进一步调使用filter、group、cross等方法"""
        table = self[table]
        raw = self.session.query(table)
        return Query(table, raw)

    def gxj(self, output_by, date_type, period, **kwargs):
        """
        供销价
        :param output_by: 'trend', 'plate'
        :param date_type: 'week', 'month'
        :param period: int
        :param kwargs: usage, plate, other filter kargs
        :return:  Gxj object
        """
        return Gxj(output_by, date_type, period, **kwargs)

    def rank(self, table_name, period, group_by, outputs, **filter_kwargs):
        # 参数
        date_range, _, _, _ = self.render_date(table_name.split('_')[0], period)
        outputs = [outputs] if isinstance(outputs, str) else outputs
        outputs_add = [x for x in ['面积', '金额'] if x not in outputs] if '均价' in outputs else []

        # 筛选、分组、生成df
        q = self.query(table_name).filter(date_range=date_range, **filter_kwargs).group(group_by, outputs + outputs_add)
        df = q.df().sort_values(outputs[0], ascending=False)

        # 计算均价删除多余列
        if '均价' in outputs:
            df.均价 = round(df.金额 / df.面积)
        for key in ['面积', '金额']:
            if (key in df) and (key not in outputs):
                df.drop(key, axis=1, inplace=True)

        # 加入名次列
        df.reset_index(drop=True, inplace=True)
        df['rank'] = df.index + 1
        columns = df.columns.tolist()
        columns.insert(0, columns.pop())

        return df[columns]


class Query:
    __by_name = {'acreage': '面积段', 'aveprice': '单价段', 'tprice': '总价段'}

    def __init__(self, table, raw):
        self.raw = raw
        self.table = table

    def filter(self, **kwargs):
        """筛选"""

        # 根据传入条件为字符串或可迭代执行不同筛选方法
        def equale_or_in(arg, col):
            col = getattr(self.table.c, col)
            if isinstance(arg, str):
                self.raw = self.raw.filter(col == arg)
            else:
                self.raw = self.raw.filter(col.in_(arg))

        # 简化日期的选择
        if 'date_range' in kwargs:
            date_field = getattr(self.table.c, '星期' if 'week' in self.table.name else '年月')
            self.raw = self.raw.filter(date_field.between(*kwargs['date_range']))

        # 简化功能、板块、片区、推广名、备案名的选择
        keys = ['usage', 'plate', 'zone', 'popname', 'prjname']
        columns = ['功能', '板块', '片区', 'popularizename', 'projectname']
        for key, column in zip(keys, columns):
            if key in kwargs:
                equale_or_in(kwargs[key], column)

        return self

    def group(self, by, outputs):
        """分组聚合"""

        def col(x):
            return getattr(self.table.c, x)

        # 设置分组依据
        if isinstance(by, str):
            by_fields = [col(by)]
            outputs_fields = [col(by).label(by)]
        else:
            by_fields = [col(x) for x in by]
            outputs_fields = [col(x).label(x) for x in by]

        # 设置输出项目，按sum聚合
        if isinstance(outputs, str):
            outputs_fields.extend([func.sum(col(outputs)).label(outputs)])
        else:
            outputs_fields.extend([func.sum(col(x)).label(x) for x in outputs])

        # 执行分组聚合
        self.raw = self.raw.from_self(*outputs_fields).group_by(*by_fields)

        return self

    def _cut_label(self, by, bins, labels=None):
        """返回一个带区间标记的df_，用以与左表join"""
        df_ = WinsunDatabase().query(by).df()
        df_.drop(self.__by_name[by], axis=1, inplace=True)
        df_.columns = [self.__by_name[by] if x == 'id' else x for x in df_]

        # 如果没有传入labels，则自动生成
        if not labels:
            labels = []
            for i, label in enumerate(bins):
                if i == 0:
                    labels.append(f'{label}-')
                else:
                    labels.append(f'{bins[i-1]}-{label}')
            labels.append(f'{bins[-1]}+')

        # 对bins添加一个最小值0，与一个最值， 如果传入总价段，将万元换算成元
        bins.insert(0, 0)
        bins.append(df_[f'{by}_high'].max() + 1)
        if by == 'tprice':
            bins = [x * 1e4 for x in bins]

        df_[f'{by}_range'] = pd.cut(df_[f'{by}_high'], bins, labels=labels)

        return df_.drop(f'{by}_low', axis=1).drop(f'{by}_high', axis=1)

    def cut(self, by, bins, labels=None, columns=None):
        df = pd.merge(self.df(), self._cut_label(by, bins, labels))
        return df.drop(self.__by_name[by], axis=1).pivot_table(index=f'{by}_range', columns=columns, aggfunc='sum')

    def cross(self, values, idx_cols, idx_bins, cols_bins):
        """分段交叉分析，在query.filter之后使用
        :param: values: 需聚合显示的输出数据名或列表，如['面积', '件数']
        :param idx_cols: 'acreage', 'aveprice', 'tprice' 三项中选两项组成列表，如['acreage', 'aveprice']
        :param idx_bins, cols_bins: 分别传入与idx_cols中两项对应的bins列表
        :return df: DataFrame object
        """
        # 在filter之后调用group与df
        idx, cols = idx_cols
        df = self.group([self.__by_name[idx], self.__by_name[cols]], values).df()

        # 调整表格
        df = pd.merge(df, self._cut_label(idx, idx_bins))
        df = pd.merge(df, self._cut_label(cols, cols_bins))
        df = df.drop(self.__by_name[idx], axis=1).drop(self.__by_name[cols], axis=1)

        return df.pivot_table(index=f'{idx}_range', columns=f'{cols}_range', aggfunc='sum')

    def df(self, index=None, column=None):
        """将查询数据转换为DataFrame，可按行列进行透视"""
        _df = pd.read_sql(self.raw.statement, self.raw.session.bind)
        _df = _df.replace('仙西', '仙林')

        if index and column:
            return _df.pivot(index, column)
        elif index:
            return _df.set_index(index)
        else:
            return _df


class Gxj:
    def __init__(self, output_by, date_type, period, **kwargs):
        self.date_range, self.group_by, self.index_sql, self.index_label = WinsunDatabase().render_date(date_type,
                                                                                                        period)
        self.group_by = '板块' if output_by == 'plate' else self.group_by
        self.date_type = date_type
        self.kwargs = kwargs

    def filter(self):
        """按日期等条件筛选"""
        for table in ['sale', 'sold']:
            yield WinsunDatabase().query(f'{self.date_type}_{table}').filter(date_range=self.date_range, **self.kwargs)

    def group(self):
        """按日期或板块进行分组"""

        def join(sale, sold):
            """合并上市、成交两张表"""
            sale.columns = ['sale']
            sold.columns = ['sold', 'money']
            sold['price'] = sold.money / sold.sold
            return sale.join(sold[['sold', 'price']], how='outer')

        sale, sold = self.filter()
        sale = sale.group(self.group_by, '面积').df(index=self.group_by)
        sold = sold.group(self.group_by, ['面积', '金额']).df(index=self.group_by)
        return join(sale, sold)

    def trend(self):
        df = self.group()
        df = df.reindex(self.index_sql)
        df.index = self.index_label

        return df

    def plate(self):
        df = self.group()
        df = df.reindex(self.kwargs['plate']) if 'plate' in self.kwargs else df.reindex(QUANSHI_)

        return df

    def shuoli(self, degree, tb_period=None):
        return Shuoli(self.df_original, self.df_adjusted, degree, tb_period)

    @property
    def df_original(self):
        return self.plate() if self.group_by == '板块' else self.trend()

    @property
    def df_adjusted(self):
        df = self.df_original.copy()
        df.sale = df.sale / 1e4
        df.sold = df.sold / 1e4
        df = df.round(2)
        df.price = df.price.round()
        return df


class Shuoli:
    def __init__(self, df_original, df_ajusted, degree, tb_period):
        pct_change = partial(gr2change, degree=degree)
        df_h = df_original.pct_change().applymap(pct_change)

        # 调整用参数
        rows = [df_ajusted.iloc[-1:], df_h.iloc[-1:]]
        index = ['v', 'h']

        # 同比
        if tb_period:
            df_t = df_original.pct_change(tb_period).applymap(pct_change)
            rows.append(df_t.iloc[-1:])
            index.append('t')

        self.df = pd.concat(rows)
        self.df.index = index

    def __thb_txt(self, item):
        series = self.df[item]

        hb = f'，环比{series.h}' if series.h else ''

        if 't' in series:
            tb = f'，同比{series.t}' if series.t else ''
            return f'{hb}{tb}'

        else:
            return hb

    def __value_txt(self, item):
        value = self.df[item].v

        # 上市、成交、认购
        if item in ['sale', 'sold']:
            if not value:
                return f'无'
            else:
                return f'{value:.2f}万㎡'

        #  均价
        else:
            if not value:
                return '无'
            else:
                return f'{value:.0f}元/㎡'

    def text(self, item):
        value = self.__value_txt(item)

        # 上市、成交、认购
        if item in ['sale', 'sold']:
            item_ = {'sale': '上市', 'sold': '成交'}[item]
            if value == '无':
                return f'无{item_}。'
            else:
                thb = self.__thb_txt(item)
                return f'{item_}{value}{thb}。'
        # 均价
        else:
            if value == '无':
                return ''
            else:
                thb = self.__thb_txt(item)
                return f'成交均价{value}{thb}。'

    @property
    def full_text(self):
        text = ''

        for each in ['sale', 'sold', 'price']:
            text += self.text(each)

        return text
