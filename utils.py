from openpyxl import load_workbook
from openpyxl import Workbook



def excel_to_list_dict(file_path, sheet_name=None):
    wb = load_workbook(file_path, data_only=True)
    ws = wb[sheet_name] if sheet_name else wb.active

    rows = ws.iter_rows(values_only=True)

    # 第一行作为 key
    headers = next(rows)
    if not headers:
        return []

    data = []
    for row in rows:
        # 跳过空行
        if all(cell is None for cell in row):
            continue
        
        row_list = list(row)
        for i, val in enumerate(row_list):
            if val is not None:
                # 1. Handle very large numbers as strings (IDs, barcodes)
                # Excel/JS precision limit starts at ~15-16 digits. We use 12 as safety.
                if isinstance(val, (int, float)):
                    abs_val = abs(val)
                    if abs_val >= 10**12:
                        # Convert to int then str to remove ANY scientific notation or decimals
                        row_list[i] = str(int(val))
                    elif isinstance(val, float):
                        if val == int(val):
                            row_list[i] = int(val)
                        else:
                            # Round common floats (similarity, price) to 4 decimals for cleaner internal data
                            row_list[i] = round(val, 4)

        data.append(dict(zip(headers, row_list)))

    return data


def write_dict_list_to_excel(data, file_path="output_text.xlsx"):
    if not data:
        print("Empty data, nothing to write")
        return

    # 创建工作簿
    wb = Workbook()
    ws = wb.active
    ws.title = "Sheet1"

    # 写入表头
    headers = list(data[0].keys())
    ws.append(headers)

    # 写入每一行
    for item in data:
        row = []
        for h in headers:
            val = item.get(h, "")
            # Global Numeric Optimization for Writing
            if isinstance(val, (int, float)):
                abs_val = abs(val)
                if abs_val >= 10**12:
                    # Large IDs MUST be strings in Excel to avoid scientific notation and precision loss
                    val = str(int(val))
                elif isinstance(val, float):
                    if val == int(val):
                        val = int(val)
                    else:
                        # Rounding for prices/similarity tails
                        val = round(val, 4)
            row.append(val)
        ws.append(row)

    # 保存文件
    wb.save(file_path)
    print(f"Excel saved to {file_path}")
