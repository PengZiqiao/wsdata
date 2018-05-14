import json
from datetime import date

from .models import WinsunDatabase, Week
from .utils import Spider

path = 'E:/gisDataBase'


def str2date(string):
    return date(*[int(x) for x in string.split('-')])


class Update(Spider):
    ws = WinsunDatabase()

    def load(self, filename):
        """打开一个json文件,返回object"""
        with open(f'{path}/{filename}.json', 'r') as f:
            return json.load(f)

    def write(self, text, type_, date_, table):
        """将获得的json文本写入文件"""
        with open(f'{path}/{type_}_{table}/{date_}.json', 'w') as f:
            f.write(text)
        print(f'>>> 【{type_} {table} {date_}】is written into file.')

    def login(self):
        """登录gis,设置cookies和headers"""
        url = 'winsun.house365.com'
        self.set_cookies(f'http://{url}')
        self.session.headers.update({'Host': url, 'Referer': f'http://{url}/'})

    def get(self, type_, date_, table):
        """获得市场数据
        :param type_: 'month' or 'week'
        :param date_: '2018-01-01' for month or '201801' for week
        :param table: 'sale', 'book', 'sold'
        :return: result: json 形式的 string
        """
        url = 'http://winsun.house365.com/sys/dataout/data'
        args = {'type': type_, 't1': date_, 't2': date_, 't': table}
        result = self.session.get(url, params=args)
        print(f'>>> 【{type_} {table} {date_}】get!')
        return result.content.replace(b'\xef\xbb\xbf', b'').decode()

    def market(self, obj, type_, table):
        session = self.ws.session
        for i, rec in enumerate(obj):
            for key in ['年月', 'perdate', 'start_date', 'end_date']:
                if key in rec:
                    obj[i][key] = str2date(obj[i][key])

        session.execute(self.ws[f'{type_}_{table}'].insert(), obj)
        session.commit()

    def get_write_update(self, type_, date_, table):
        """get、写入文件、 更新数据库"""
        text = self.get_write(type_, date_, table)
        obj = json.loads(text)
        print(f'>>> 添加{len(obj)}条记录。')
        self.market(obj, type_, table)

    def get_write(self, type_, date_, table):
        """get并写入文件"""
        text = self.get(type_, date_, table)
        self.write(text, type_, date_, table)
        return text


if __name__ == '__main__':
    ud = Update()
    ud.login()
    
    print(f'>>> 周报应更新到 {Week()}')
    type_ = input('>>> 请输入更新报表类型(week/month)...\n')
    date_ = input('>>> 请输入更新日期(如201802/2018-02-01)...\n')
    
    for table in ['sale', 'book', 'sold']:
        ud.get_write_update(type_, date_, table)
