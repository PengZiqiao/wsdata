import datetime
from pandas.tseries.offsets import DateOffset, MonthBegin
from numpy import isnan
from selenium import webdriver
import requests


class Week:
    """
    monday, sunday 为上周一、日
    N 为上周周数
    """
    # 从今天开始往前找第一个星期日
    sunday = datetime.date.today()
    while sunday.weekday() != 6:
        sunday -= datetime.timedelta(days=1)

    # 从上周星期日往前推6天即为星期一
    monday = sunday - datetime.timedelta(days=6)

    str_format = '%Y%m%d'

    @property
    def sunday_str(self):
        return self.sunday.strftime(self.str_format)

    @property
    def monday_str(self):
        return self.monday.strftime(self.str_format)

    # 周数
    @property
    def N(self):
        return int(self.sunday.strftime('%U')) if self.monday.year == 2018 else int(self.monday.strftime('%U'))

    @property
    def string(self):
        return int(f'{self.monday.year}{self.N:02d}')

    def before(self, i):
        """从上周起往前的第i周"""
        w = Week()
        w.monday = self.monday - datetime.timedelta(weeks=i)
        w.sunday = self.sunday - datetime.timedelta(weeks=i)
        return w

    def __repr__(self):
        return f'<class Week: {self.monday_str}-{self.sunday_str} {self.N}>'


class Month:
    str_format = "%Y%m%d"

    def __init__(self):
        last_month = datetime.date.today() - DateOffset(months=1)
        # 25号（不含）之后运行，月份为当月，1号至25号（含）运行，月份为上个月
        self.date = MonthBegin().rollforward(last_month) if last_month.day > 25 else MonthBegin().rollback(last_month)
        self.month = self.date.month
        self.year = self.date.year

    @property
    def string(self):
        return self.date.strftime(self.str_format)

    def before(self, i):
        """i个月之前"""
        m = Month()
        m.date = self.date - DateOffset(months=i)
        m.month = m.date.month
        m.year = m.date.year
        return m

    def __repr__(self):
        return f'<class Month: {self.string}>'


def growth_rate(a, b):
    """growth rate
    a相对于b的增长率
    """
    return 0 if (isnan(b) or b == 0) else (a - b) / b


def change(a, b, degree=2):
    """
    以“增长/下降xx%”的形式返回a对于b的增长率
    """
    return gr2change(growth_rate(a, b), degree)


def gr2change(value, degree=2):
    """
    将带正负号的比值(1代表100%)转成“增长/下降xx%”的形式
    """
    if isnan(value):
        # 传入nan时直接返回空字符串
        return ''
    else:
        # 方向
        chg = '下降' if value < 0 else '增长'
        # 数值
        value = abs(value) * 100
        value = f'{value:.0f}' if degree == 0 else round(value, degree)
        # 组合
        return f'{chg}{value}%'


def wan(x):
    return round(x / 1e4, 2)


class Spider:
    def __init__(self):
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko)',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            'Connection': 'keep-alive'
        }

        # selenium
        options = webdriver.ChromeOptions()
        for key, value in headers.items():
            options.add_argument(f'{key}={value}')
        self.driver = webdriver.Chrome(chrome_options=options)

        # session
        self.session = requests.Session()
        self.session.headers = headers

    def set_cookies(self, url):
        """通过selenium登陆后获得cookies，并设定至requests.Session()"""
        self.driver.get(url)
        input('>>> 登陆完成后请按回车...')
        cookies = self.driver.get_cookies()
        cookies = dict((each['name'], each['value']) for each in cookies)
        self.session.cookies = requests.utils.cookiejar_from_dict(cookies)
