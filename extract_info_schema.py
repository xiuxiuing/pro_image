from typing import Literal, get_args
from pydantic import BaseModel, Field


DEFAULT_MOONSHOT_MODEL = "kimi-k2-turbo-preview"
MOONSHOT_BASE_URL = "https://api.moonshot.cn/v1"

PackagingUnit = Literal[
    "袋",
    "盒",
    "瓶",
    "罐",
    "桶",
    "箱",
    "听",
    "杯",
    "碗",
    "支",
    "条",
    "片",
    "套",
    "枚",
    "粒",
    "颗",
    "个",
    "只",
    "包",
    "件",
    "份",
    "把",
    "本",
    "台",
    "床",
    "顶",
    "贴",
    "块",
    "卡",
    "根",
    "张",
    "双",
    "副",
    "对",
    "板",
    "组",
    "卷",
    "团",
    "未知",
]


A_FIELD_COLUMNS = [
    "A核心品类",
    "A单件净含量",
    "A售卖数量",
    "A包装单位",
    "A尺寸",
    "A多维尺寸",
    "A品牌",
    "A型号",
    "A商品形态",
    "A关键属性词",
    "A颜色",
]

STRONG_A_FIELDS = frozenset(
    [
        "A核心品类",
        "A单件净含量",
        "A售卖数量",
        "A包装单位",
        "A尺寸",
        "A多维尺寸",
    ]
)

WEAK_A_FIELDS = frozenset(["A品牌", "A型号", "A商品形态", "A关键属性词", "A颜色"])
SELL_SPEC_FIELDS = ("A售卖数量", "A包装单位")


class ProductInfo(BaseModel):
    """
    V2 可比价字段抽取结构（写回 Excel 的 11 列 A*）。

    说明：品牌仅作为弱排序信号。强约束字段用于后续规格/品类判断，弱排序字段只做归一和排序信号。
    """

    net_content: str = Field(default="", description="单件净含量，标准单位 ml/L/g/kg，如 330ml/1.5L/18g")
    sell_quantity: str = Field(default="", description="售卖数量值，只保留数字，如 24/6/7/2/1，不带罐/瓶/个等单位")
    packaging_unit: PackagingUnit = Field(default="未知", description="包装单位，如 罐/瓶/袋/片/条/个/箱/包；不确定填 未知")
    color: list[str] = Field(default_factory=list, description="颜色（弱排序字段，可多值，不用于硬拦截）")
    size: list[str] = Field(default_factory=list, description="一维尺寸/长度/尺码，如 240mm/80-105cm/XL；长宽高放入 A多维尺寸")
    brand: str = Field(default="", description="品牌（弱排序字段），只取明确真实品牌；不确定留空，不用于硬拦截")
    model: str = Field(default="", description="型号/货号（弱排序字段，如 AB-123、X1），不确定留空；尺码不要放这里")
    core_category: str = Field(default="", description="核心品类，短商品名词，如 热熔胶棒/固体胶/洗发水/床品四件套")
    product_form: str = Field(default="", description="商品形态（弱排序字段），短词，如 液体/膏体/棒状/片状/套装/器具/耗材")
    key_attributes: list[str] = Field(default_factory=list, description="关键属性词（弱排序字段），如 一次性/双人/四件套/儿童/无糖/低温")
    multidim_size: list[str] = Field(default_factory=list, description="多维尺寸/长宽高，如 2.2x2.4m/25x30cm/9x9x3.5cm")

class BatchResponse(BaseModel):
    items: list[ProductInfo]


ALLOWED_PACKAGING = frozenset(get_args(PackagingUnit))
EXTRACTION_SOURCE_COL = "A提取来源"
MODEL_EXTRACTION_SOURCE = "模型提取"
RULE_EXTRACTION_SOURCE = "规则兜底"
