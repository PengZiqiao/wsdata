# 功能
ZHUZHAI = ('多层住宅', '高层住宅', '小高层住宅')
BIESHU = ('叠加别墅', '独立别墅', '双拼别墅', '联排别墅')
GONGYU = ('挑高公寓办公', '平层公寓办公')
XIEZILOU = ('乙级办公', '甲级办公')
SHANGYE = ('底商商业', '专业市场商业', '集中商业', '街区商业', '其它商业')

SPZZ = ZHUZHAI + BIESHU
BANGONG = GONGYU + XIEZILOU + ('其它办公',)

# 板块
QUANSHI = ('城中', '城东', '城南', '河西', '城北', '仙西', '江宁', '浦口', '江北新区直管区', '六合', '溧水', '高淳')
QUANSHI_ = ['仙林' if x == '仙西' else x for x in QUANSHI]
BUHANLIGAO = QUANSHI[:-2]
BUHANLIGAOLU = BUHANLIGAO[:-1]
