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
    "支",
    "条",
    "片",
    "套",
    "枚",
    "个",
    "只",
    "包",
    "件",
    "板",
    "组",
    "卷",
    "未知",
]

class ProductInfo(BaseModel):
    """
    Gemini 新提取结构（写回 Excel 的新 6 列 A*）。

    说明：不再提取 品牌、核心名称、外观、材质。
    """

    net_content: str = Field(default="", description="单件净含量，标准单位 ml/L/g/kg，如 330ml/1.5L/18g")
    sell_quantity: str = Field(default="", description="售卖数量值，只保留数字，如 24/6/7/2/1，不带罐/瓶/个等单位")
    packaging_unit: PackagingUnit = Field(default="未知", description="包装单位，如 罐/瓶/袋/片/条/个/箱/包；不确定填 未知")
    color: list[str] = Field(default_factory=list, description="颜色（可多值）")
    size: list[str] = Field(default_factory=list, description="尺寸/长度/码数（可多值，如 240mm/17x25x8cm/XL）")
    model: str = Field(default="", description="型号（如 AB-123、X1），不确定留空")

class BatchResponse(BaseModel):
    items: list[ProductInfo]


ALLOWED_PACKAGING = frozenset(get_args(PackagingUnit))
EXTRACTION_SOURCE_COL = "A提取来源"
MODEL_EXTRACTION_SOURCE = "模型提取"
RULE_EXTRACTION_SOURCE = "规则兜底"
