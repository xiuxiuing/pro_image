from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from utils import clean_text_value


CORE_CATEGORY_SYNONYM_GROUPS: List[List[str]] = [
    ["猫条", "猫零食条", "猫咪零食条", "宠物猫条"],
    ["猫零食", "宠物零食"],
    ["膨润土猫砂", "膨润土砂"],
    ["豆腐猫砂", "豆腐砂"],
    ["矿砂猫砂", "矿物猫砂", "矿物砂"],
    ["混合猫砂", "混合砂"],
    ["猫砂除臭珠", "除臭珠", "猫砂伴侣"],
    ["猫砂", "猫沙"],
    ["宠物主粮", "猫粮", "狗粮", "犬粮"],
    ["宠物罐头", "猫罐头", "狗罐头", "犬罐头"],
    ["宠物冻干", "猫冻干", "狗冻干", "冻干零食"],
    ["宠物磨牙棒", "狗狗磨牙棒", "洁齿棒"],
    ["宠物手套", "宠物洗澡手套", "宠物清洁手套", "宠物免洗手套"],
    ["宠物湿巾", "宠物清洁湿巾"],
    ["宠物沐浴露", "宠物香波", "猫狗沐浴露"],
    ["宠物梳子", "宠物梳", "针梳", "排梳"],
    ["宠物碗", "猫碗", "狗碗", "宠物食盆"],
    ["裤型卫生巾", "安睡裤", "安心裤", "夜安裤", "卫生巾安睡裤"],
    ["卫生巾", "卫生棉", "姨妈巾"],
    ["卫生护垫", "护垫", "迷你巾"],
    ["棉条", "卫生棉条", "导管棉条"],
    ["美纹纸胶带", "美纹纸", "遮蔽胶带", "分色纸胶带"],
    ["透明胶带", "透明胶", "封箱胶带", "封口胶带"],
    ["双面胶", "双面胶带"],
    ["胶带", "胶布"],
    ["热熔胶棒", "热熔胶条"],
    ["固体胶", "固体胶棒"],
    ["液体胶", "胶水"],
    ["白乳胶", "乳胶"],
    ["502胶水", "瞬干胶", "快干胶"],
    ["含乳饮料", "乳饮料", "乳酸菌饮料", "水果牛奶饮料", "水果酸奶饮品", "营养快线"],
    ["包装饮用水", "饮用水", "纯净水", "天然泉水", "矿泉水", "天然矿泉水"],
    ["碳酸饮料", "汽水"],
    ["茶饮料", "冰红茶", "绿茶饮料", "乌龙茶饮料"],
    ["果汁饮料", "果汁", "果味饮料"],
    ["果蔬汁饮料", "果蔬汁", "西梅汁", "苹果汁"],
    ["功能饮料", "能量饮料", "运动饮料"],
    ["咖啡饮料", "即饮咖啡"],
    ["火鸡面", "方便面", "泡面", "速食面", "拌面"],
    ["螺蛳粉", "方便螺蛳粉"],
    ["酸辣粉", "方便酸辣粉"],
    ["自热火锅", "自嗨锅", "自热锅"],
    ["饼干", "曲奇", "夹心饼干"],
    ["薯片", "膨化薯片"],
    ["坚果", "混合坚果", "每日坚果"],
    ["糖果", "软糖", "硬糖", "薄荷糖", "润喉糖"],
    ["巧克力", "巧克力制品"],
    ["鱼干", "小鱼干", "即食鱼干"],
    ["辣条", "面筋", "辣棒", "调味面制品", "面筋制品"],
    ["洗发水", "洗发露", "洗发乳", "洗发液"],
    ["护发素", "润发乳", "润发素"],
    ["沐浴露", "沐浴乳", "沐浴液"],
    ["洗面奶", "洁面乳", "洁面膏", "洁面泡沫"],
    ["香皂", "肥皂", "洁肤皂"],
    ["牙膏", "牙膏膏体"],
    ["牙刷", "软毛牙刷", "电动牙刷刷头"],
    ["漱口水", "漱口液"],
    ["洗手液", "洗手露", "泡沫洗手液"],
    ["洗衣液", "洗衣凝珠", "洗衣珠"],
    ["洗衣粉", "洗衣皂粉"],
    ["柔顺剂", "衣物柔顺剂"],
    ["消毒液", "除菌液"],
    ["洁厕剂", "洁厕液", "洁厕灵"],
    ["油污净", "厨房重油污净", "油污清洁剂"],
    ["空气清新剂", "空气清香剂", "除味喷雾"],
    ["抽纸", "面巾纸", "纸巾", "餐巾纸"],
    ["卷纸", "卫生纸", "厕纸", "卷筒纸"],
    ["湿巾", "清洁湿巾", "手口湿巾"],
    ["垃圾袋", "清洁袋"],
    ["保鲜袋", "食品袋"],
    ["保鲜膜", "食品保鲜膜"],
    ["礼袋", "礼品袋", "手提袋", "包装袋", "服装纸袋", "纸袋", "牛皮纸袋", "购物袋", "手提纸袋"],
    ["红包袋", "红包", "利是封"],
    ["礼盒", "礼品盒", "包装盒", "喜糖盒", "喜糖礼盒"],
    ["拉菲草", "填充物", "包装填充物", "碎纸丝", "纸丝"],
    ["收纳袋", "整理袋", "衣物收纳袋", "搬家袋", "编织袋", "行李袋", "储物袋"],
    ["真空压缩袋", "压缩袋"],
    ["收纳盒", "整理盒", "储物盒"],
    ["收纳箱", "整理箱", "储物箱"],
    ["自封袋", "密封袋", "封口袋", "密实袋", "自粘袋", "OPP自粘袋"],
    ["气泡袋", "气泡信封袋", "防震袋", "气柱袋", "气泡膜"],
    ["食品包装袋", "食品袋", "食品纸袋", "打包袋", "外卖袋", "防油纸袋", "小吃包装袋"],
    ["脸盆", "面盆", "洗脸盆"],
    ["水桶", "塑料桶", "提桶"],
    ["保鲜盒", "饭盒", "便当盒"],
    ["水杯", "杯子", "随行杯"],
    ["一次性杯子", "纸杯", "塑料杯"],
    ["一次性餐具", "一次性筷子", "一次性勺子", "一次性叉子"],
    ["手套", "一次性手套", "防护手套"],
    ["桌布", "一次性桌布", "餐桌布"],
    ["托盘", "方盘"],
    ["保鲜膜", "食品保鲜膜", "封口膜", "打包膜", "缠绕膜"],
    ["锡纸碗", "铝箔盒", "锡纸盒"],
    ["铝箔纸", "锡纸"],
    ["床品四件套", "四件套", "床上四件套"],
    ["床品三件套", "三件套", "床上三件套"],
    ["床单", "床笠"],
    ["被套", "被罩"],
    ["枕套", "枕头套"],
    ["被子", "棉被", "夏凉被", "空调被"],
    ["蚊帐", "蒙古包蚊帐"],
    ["床垫", "床褥", "床垫/床褥"],
    ["丝袜", "情趣丝袜", "蕾丝丝袜", "打底袜", "美腿袜", "长筒袜", "过膝袜", "长筒过膝袜"],
    ["内裤", "女士内裤", "男士内裤"],
    ["文胸", "胸罩", "内衣"],
    ["袜子", "棉袜", "短袜", "船袜"],
    ["干发帽", "浴帽", "包头巾", "干发巾"],
    ["浴巾干发帽套装", "浴巾套装", "干发帽套装"],
    ["毛巾", "洗脸巾", "面巾"],
    ["浴巾", "大浴巾"],
    ["口罩", "非医用口罩", "一次性口罩"],
    ["灯笼", "纸灯笼", "红灯笼"],
    ["气球", "数字气球", "铝膜气球", "铝箔气球"],
    ["蜡烛", "生日蜡烛", "数字蜡烛", "酥油灯"],
    ["圣诞花环", "装饰花环", "花环"],
    ["圣诞挂饰", "装饰挂件", "挂饰"],
    ["贴纸", "门贴", "喜字贴", "窗贴"],
    ["雨靴", "雨鞋", "水靴", "胶鞋"],
    ["拖鞋", "棉拖鞋", "男士拖鞋", "女士拖鞋", "儿童拖鞋"],
    ["连衣裙", "吊带连衣裙"],
    ["发饰", "发夹", "发卡", "头饰"],
    ["皮带", "腰带", "腰带/皮带/腰链", "裤带"],
    ["帽子", "棒球帽", "鸭舌帽", "防晒帽", "遮阳帽"],
    ["手机膜", "钢化膜", "保护膜", "手机贴膜"],
    ["皮鞋", "男士皮鞋", "商务皮鞋"],
    ["运动鞋", "休闲鞋", "板鞋", "老爹鞋"],
    ["宠物航空箱", "航空箱", "托运箱"],
    ["宠物包", "猫包", "宠物背包", "猫狗双肩包"],
    ["宠物围栏", "宠物栅栏", "狗狗围栏", "狗笼子", "宠物笼", "猫笼", "猫笼子", "宠物笼子"],
    ["宠物尿垫", "宠物尿片", "狗狗尿垫", "纸尿垫", "隔尿垫"],
    ["猫砂盆", "猫沙盆"],
    ["宠物挡板", "缝隙挡板", "床底挡板", "封床底挡板", "防猫挡板"],
    ["手机壳", "手机保护壳", "保护套"],
    ["充电器", "充电头", "电源适配器"],
    ["数据线", "充电线", "连接线"],
    ["插排", "插线板", "排插", "接线板"],
    ["鼠标", "无线鼠标", "有线鼠标"],
    ["键盘", "机械键盘", "无线键盘"],
    ["无线网卡", "网卡", "wifi接收器"],
    ["中性笔", "签字笔", "水笔"],
    ["秀丽笔", "纤秀笔"],
    ["自动铅笔", "活动铅笔"],
    ["铅笔", "木杆铅笔"],
    ["马克笔", "记号笔"],
    ["橡皮", "橡皮擦"],
    ["避孕套", "安全套", "计生用品"],
    ["润滑液", "人体润滑液", "润滑剂"],
    ["纸尿裤", "尿不湿", "纸尿片"],
    ["拉拉裤", "成长裤", "学步裤"],
    ["护理垫", "隔尿护理垫", "成人护理垫"],
    ["发胶", "头发定型", "定型喷雾"],
]

FORM_SYNONYM_GROUPS: List[List[str]] = [
    ["液体", "液态", "水状", "液状", "流体", "喷雾", "喷剂", "瓶装", "罐装", "杯装", "袋装液体"],
    ["膏体", "膏状", "乳膏", "凝胶", "啫喱", "霜状", "乳状"],
    ["粉末", "粉状", "粉剂", "散粉"],
    ["颗粒", "颗粒状", "粒状", "珠状", "丸状"],
    ["片状", "片装", "薄片", "垫片", "贴片"],
    ["条状", "条装", "条形", "长条"],
    ["棒状", "棒装", "棒形", "胶棒"],
    ["袋装", "包袋装", "软袋", "袋型", "小袋装", "独立袋"],
    ["盒装", "盒型", "纸盒装", "礼盒装"],
    ["罐装", "听装", "易拉罐装"],
    ["瓶装", "瓶型"],
    ["卷装", "卷状", "卷筒", "卷式", "成卷"],
    ["套装", "组合装", "礼盒套装", "套组", "组合"],
    ["器具", "设备", "工具", "单件", "配件", "组件"],
    ["耗材", "消耗品"],
    ["织物", "布艺", "纺织品", "服饰", "服装", "内衣", "裤装", "配饰"],
    ["鞋类", "鞋子", "鞋履", "鞋靴"],
    ["纸品", "纸质", "纸制品"],
    ["凝珠", "珠状凝胶", "凝胶珠"],
    ["湿巾", "湿片", "湿纸巾"],
    ["喷雾", "喷瓶", "喷头瓶"],
]

ATTRIBUTE_SYNONYM_GROUPS: List[List[str]] = [
    ["一次性", "免洗", "用完即弃", "即弃", "抛弃式"],
    ["可重复", "可水洗", "可复用", "重复使用"],
    ["独立包装", "独立装", "单独包装", "独立小包"],
    ["家庭装", "大包装", "囤货装", "量贩装", "实惠装"],
    ["便携", "随身", "旅行装", "迷你装", "小样"],
    ["替换装", "补充装", "替芯", "替换芯", "补充包"],
    ["超薄", "极薄", "薄款", "轻薄", "薄型", "特薄"],
    ["加厚", "厚款", "厚型", "特厚"],
    ["加长", "长款", "延长"],
    ["大号", "大码", "大尺寸"],
    ["小号", "小码", "迷你"],
    ["日用", "日间"],
    ["夜用", "夜间", "量多夜用", "夜安"],
    ["护翼", "有翼"],
    ["无护翼", "直条"],
    ["长筒", "高筒"],
    ["中筒", "中高筒"],
    ["短筒", "船袜", "低帮"],
    ["过膝", "大腿袜", "及膝以上"],
    ["连裤", "连裤袜"],
    ["开裆", "开档"],
    ["防滑", "止滑"],
    ["防水", "防潮"],
    ["防油", "隔油"],
    ["防震", "防摔", "缓冲"],
    ["透气", "透气款"],
    ["抑菌", "抗菌", "除菌", "杀菌"],
    ["除臭", "除味", "祛味"],
    ["无香", "无香型", "原味"],
    ["有香", "香型", "香味"],
    ["无糖", "0糖", "零糖", "不含糖"],
    ["低糖", "少糖", "微糖"],
    ["无蔗糖", "0蔗糖", "零蔗糖"],
    ["低脂", "减脂"],
    ["低温", "冷藏"],
    ["常温", "室温"],
    ["热熔", "热熔型"],
    ["固体", "固体型"],
    ["液体", "液态"],
    ["成人", "成人款"],
    ["儿童", "小童", "孩童"],
    ["婴儿", "宝宝", "婴幼儿"],
    ["男女通用", "通用款", "中性"],
    ["男士", "男性"],
    ["女士", "女性"],
    ["医用", "医疗级"],
    ["食品级", "可接触食品"],
    ["密封", "封口", "密实"],
    ["保温", "保冷"],
    ["即食", "开袋即食"],
    ["香辣", "辣味", "麻辣"],
    ["卤味", "辣卤"],
    ["无痕", "隐形"],
    ["蕾丝", "蕾丝边"],
    ["磨砂", "哑光"],
    ["透明", "全透", "无色"],
]

MODEL_SYNONYM_GROUPS: List[List[str]] = [
    ["iPhone", "苹果手机", "苹果"],
    ["Type-C", "TypeC", "USB-C", "USBC", "C口"],
    ["Micro USB", "MicroUSB", "安卓口"],
    ["Lightning", "苹果口", "8pin", "8-pin"],
    ["WiFi", "WIFI", "无线"],
    ["2.4G", "2.4GHz"],
    ["5G", "5GHz"],
    ["M-L", "M/L", "M码-L码", "M-L码"],
    ["L-XL", "L/XL", "L码-XL码", "L-XL码"],
    ["XL-XXL", "XL/XXL", "XL码-XXL码"],
    ["均码", "均码款", "通码"],
]

BRAND_SYNONYM_GROUPS: List[List[str]] = [
    ["可口可乐", "Coca-Cola", "Coca Cola", "可口"],
    ["百事", "百事可乐", "Pepsi"],
    ["雪碧", "Sprite"],
    ["娃哈哈", "哇哈哈"],
    ["农夫山泉", "农夫"],
    ["怡宝", "Cestbon"],
    ["景田", "Ganten"],
    ["康师傅"],
    ["统一"],
    ["蒙牛"],
    ["伊利"],
    ["特仑苏"],
    ["光明"],
    ["三只松鼠"],
    ["百草味"],
    ["良品铺子"],
    ["奥利奥", "Oreo"],
    ["乐事", "Lay's", "Lays"],
    ["溜溜梅"],
    ["劲仔"],
    ["得力", "Deli"],
    ["晨光", "M&G", "MG"],
    ["齐心", "Comix"],
    ["维达", "Vinda"],
    ["洁柔", "C&S", "CS"],
    ["清风"],
    ["心相印"],
    ["蓝月亮"],
    ["立白"],
    ["雕牌"],
    ["汰渍", "Tide"],
    ["超能"],
    ["威露士", "Walch"],
    ["滴露", "Dettol"],
    ["海飞丝", "Head & Shoulders", "海飞丝HeadShoulders"],
    ["清扬", "Clear"],
    ["潘婷", "Pantene"],
    ["飘柔", "Rejoice"],
    ["多芬", "Dove"],
    ["舒肤佳", "Safeguard"],
    ["云南白药"],
    ["黑人", "好来"],
    ["高洁丝", "Kotex"],
    ["护舒宝", "Whisper"],
    ["苏菲", "Sofy"],
    ["子初"],
    ["Babycare", "babycare"],
    ["帮宝适", "Pampers"],
    ["好奇", "Huggies"],
    ["杜蕾斯", "Durex"],
    ["冈本", "Okamoto"],
    ["名流"],
    ["小米", "MI", "Xiaomi"],
    ["华为", "HUAWEI"],
    ["苹果", "Apple", "iPhone"],
    ["品胜", "Pisen"],
    ["公牛", "Bull"],
    ["飞利浦", "Philips"],
    ["美的", "Midea"],
    ["苏泊尔", "Supor"],
    ["九阳", "Joyoung"],
    ["雷达"],
    ["榄菊"],
    ["超威"],
]

HIGH_RISK_CORE_CONFLICT_PAIRS: List[Tuple[str, str]] = [
    ("热熔胶棒", "固体胶"),
    ("热熔胶棒", "液体胶"),
    ("热熔胶棒", "502胶水"),
    ("固体胶", "液体胶"),
    ("固体胶", "502胶水"),
    ("美纹纸胶带", "透明胶带"),
    ("双面胶", "透明胶带"),
    ("胶带", "胶水"),
    ("牙膏", "牙刷"),
    ("牙膏", "洗发水"),
    ("牙膏", "护发素"),
    ("牙膏", "沐浴露"),
    ("牙膏", "洗面奶"),
    ("牙膏", "漱口水"),
    ("洗发水", "护发素"),
    ("洗发水", "沐浴露"),
    ("洗发水", "洗面奶"),
    ("沐浴露", "洗面奶"),
    ("香皂", "洗发水"),
    ("香皂", "护发素"),
    ("香皂", "沐浴露"),
    ("香皂", "洗面奶"),
    ("香皂", "牙膏"),
    ("香皂", "避孕套"),
    ("洗手液", "洗发水"),
    ("洗衣液", "洗衣粉"),
    ("洗衣液", "柔顺剂"),
    ("洗衣液", "消毒液"),
    ("洁厕剂", "油污净"),
    ("抽纸", "卷纸"),
    ("抽纸", "湿巾"),
    ("卷纸", "湿巾"),
    ("垃圾袋", "保鲜袋"),
    ("垃圾袋", "保鲜膜"),
    ("保鲜袋", "保鲜膜"),
    ("礼袋", "礼盒"),
    ("礼袋", "收纳袋"),
    ("礼袋", "垃圾袋"),
    ("礼袋", "食品包装袋"),
    ("礼袋", "拉菲草"),
    ("礼盒", "拉菲草"),
    ("收纳袋", "垃圾袋"),
    ("自封袋", "真空压缩袋"),
    ("收纳盒", "收纳袋"),
    ("收纳箱", "收纳袋"),
    ("水杯", "一次性杯子"),
    ("脸盆", "水桶"),
    ("床品四件套", "床品三件套"),
    ("床单", "被套"),
    ("床单", "枕套"),
    ("被套", "枕套"),
    ("卫生巾", "裤型卫生巾"),
    ("卫生巾", "卫生护垫"),
    ("卫生巾", "棉条"),
    ("裤型卫生巾", "卫生护垫"),
    ("裤型卫生巾", "棉条"),
    ("宠物手套", "裤型卫生巾"),
    ("猫砂", "猫条"),
    ("猫砂", "猫零食"),
    ("猫砂", "宠物主粮"),
    ("猫砂", "宠物罐头"),
    ("猫砂", "宠物湿巾"),
    ("猫砂", "宠物尿垫"),
    ("猫砂", "猫砂盆"),
    ("猫砂", "宠物围栏"),
    ("猫砂", "宠物挡板"),
    ("猫条", "宠物主粮"),
    ("猫条", "宠物罐头"),
    ("宠物主粮", "宠物湿巾"),
    ("宠物湿巾", "宠物尿垫"),
    ("宠物航空箱", "宠物包"),
    ("宠物围栏", "宠物航空箱"),
    ("手机壳", "手机膜"),
    ("充电器", "数据线"),
    ("充电器", "插排"),
    ("鼠标", "键盘"),
    ("无线网卡", "数据线"),
    ("中性笔", "自动铅笔"),
    ("中性笔", "铅笔"),
    ("秀丽笔", "中性笔"),
    ("橡皮", "铅笔"),
    ("丝袜", "内裤"),
    ("丝袜", "文胸"),
    ("内裤", "文胸"),
    ("浴巾", "毛巾"),
    ("干发帽", "浴巾"),
    ("避孕套", "润滑液"),
    ("纸尿裤", "拉拉裤"),
    ("纸尿裤", "护理垫"),
    ("拉拉裤", "护理垫"),
]


def _norm_text(value: Any) -> str:
    if value is None:
        return ""
    text = clean_text_value(value)
    if text is None:
        return ""
    text = str(text).strip()
    if text.lower() in ("nan", "none", "null"):
        return ""
    return text


def _compact(value: Any) -> str:
    return re.sub(r"[\s_\-/｜|,，、（）()【】\\[\\]{}:：]+", "", _norm_text(value)).lower()


def _synonym_map(groups: Iterable[Iterable[str]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for group in groups:
        items = [_norm_text(x) for x in group if _norm_text(x)]
        if not items:
            continue
        rep = items[0]
        for item in items:
            out[item] = rep
            out[_compact(item)] = rep
    return out


_CORE_MAP = _synonym_map(CORE_CATEGORY_SYNONYM_GROUPS)
_FORM_MAP = _synonym_map(FORM_SYNONYM_GROUPS)
_ATTR_MAP = _synonym_map(ATTRIBUTE_SYNONYM_GROUPS)
_MODEL_MAP = _synonym_map(MODEL_SYNONYM_GROUPS)
_BRAND_MAP = _synonym_map(BRAND_SYNONYM_GROUPS)


def normalize_core_category(value: Any) -> str:
    raw = _norm_text(value)
    if not raw:
        return ""
    compact = _compact(raw)
    if raw in _CORE_MAP:
        return _CORE_MAP[raw]
    if compact in _CORE_MAP:
        return _CORE_MAP[compact]
    for group in CORE_CATEGORY_SYNONYM_GROUPS:
        rep = group[0]
        for token in sorted(group, key=len, reverse=True):
            token_c = _compact(token)
            if token_c and token_c in compact:
                return rep
    return raw


def core_categories_compatible(a: Any, b: Any) -> bool:
    na = normalize_core_category(a)
    nb = normalize_core_category(b)
    return not (na and nb) or na == nb


def _conflict_key(a: Any, b: Any) -> Tuple[str, str]:
    aa = normalize_core_category(a)
    bb = normalize_core_category(b)
    return tuple(sorted((aa, bb)))


_CORE_CONFLICT_SET = {
    _conflict_key(a, b)
    for a, b in HIGH_RISK_CORE_CONFLICT_PAIRS
    if normalize_core_category(a) and normalize_core_category(b)
}


def core_category_conflict_pair(a: Any, b: Any) -> Optional[Tuple[str, str]]:
    na = normalize_core_category(a)
    nb = normalize_core_category(b)
    if not na or not nb or na == nb:
        return None
    key = tuple(sorted((na, nb)))
    if key in _CORE_CONFLICT_SET:
        return (na, nb)
    return None


def normalize_product_form(value: Any) -> str:
    raw = _norm_text(value)
    if not raw:
        return ""
    compact = _compact(raw)
    if raw in _FORM_MAP:
        return _FORM_MAP[raw]
    if compact in _FORM_MAP:
        return _FORM_MAP[compact]
    for group in FORM_SYNONYM_GROUPS:
        rep = group[0]
        for token in sorted(group, key=len, reverse=True):
            token_c = _compact(token)
            if token_c and token_c in compact:
                return rep
    return raw


def split_a_tokens(value: Any) -> List[str]:
    raw = _norm_text(value)
    if not raw:
        return []
    parts = re.split(r"[,，|/、\s]+", raw)
    return [p.strip() for p in parts if p.strip()]


def normalize_key_attributes(value: Any) -> List[str]:
    out: List[str] = []
    seen = set()
    for token in split_a_tokens(value):
        norm = _ATTR_MAP.get(token) or _ATTR_MAP.get(_compact(token)) or token
        if norm and norm not in seen:
            seen.add(norm)
            out.append(norm)
    compact = _compact(value)
    for group in ATTRIBUTE_SYNONYM_GROUPS:
        rep = group[0]
        if rep in seen:
            continue
        for token in sorted(group, key=len, reverse=True):
            token_c = _compact(token)
            if token_c and token_c in compact:
                seen.add(rep)
                out.append(rep)
                break
    return out


def normalize_model(value: Any) -> str:
    raw = _norm_text(value)
    if not raw:
        return ""
    raw = re.sub(r"(?i)^(?:型号|model|货号|款号)\s*[:：]?\s*", "", raw).strip()
    compact = _compact(raw)
    if raw in _MODEL_MAP:
        return _MODEL_MAP[raw]
    if compact in _MODEL_MAP:
        return _MODEL_MAP[compact]
    return re.sub(r"\s+", "", raw).upper()


_BRAND_NOISE = {
    "品牌",
    "品牌随机",
    "随机品牌",
    "款式随机",
    "颜色随机",
    "优选",
    "优选好物",
    "网红",
    "爆款",
    "同款",
    "厂家直供",
    "新老包装随机",
    "不限品牌",
    "无品牌",
}


def normalize_brand(value: Any) -> str:
    raw = _norm_text(value)
    if not raw:
        return ""
    raw = re.sub(r"(?i)^(?:品牌|brand)\s*[:：]?\s*", "", raw).strip()
    raw = re.sub(r"（.*?）|\\(.*?\\)|【.*?】", "", raw).strip()
    if not raw or raw in _BRAND_NOISE:
        return ""
    compact = _compact(raw)
    if raw in _BRAND_MAP:
        return _BRAND_MAP[raw]
    if compact in _BRAND_MAP:
        return _BRAND_MAP[compact]
    for group in BRAND_SYNONYM_GROUPS:
        rep = group[0]
        for token in sorted(group, key=len, reverse=True):
            token_c = _compact(token)
            if token_c and token_c == compact:
                return rep
    if len(raw) > 24 or re.search(r"(随机|可选|适用|专用|同款|新款|升级|加厚|超薄|经典|清爽)", raw):
        return ""
    return raw


def weak_a_signal_summary(item: dict) -> Dict[str, Any]:
    return {
        "form": normalize_product_form(item.get("A商品形态")),
        "attributes": normalize_key_attributes(item.get("A关键属性词")),
        "color": split_a_tokens(item.get("A颜色")),
        "model": normalize_model(item.get("A型号")),
        "brand": normalize_brand(item.get("A品牌")),
    }
