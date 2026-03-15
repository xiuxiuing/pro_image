from openpyxl import load_workbook
from openpyxl import Workbook



def excel_to_list_dict(file_path, sheet_name=None):
    wb = load_workbook(file_path, data_only=True)
    ws = wb[sheet_name] if sheet_name else wb.active

    rows = ws.iter_rows(values_only=True)

    # 第一行作为 key
    headers = next(rows)

    data = []
    for row in rows:
        # 跳过空行
        if all(cell is None for cell in row):
            continue

        data.append(dict(zip(headers, row)))

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
        row = [item.get(h, "") for h in headers]  # 如果缺失某列，填空
        ws.append(row)

    # 保存文件
    wb.save(file_path)
    print(f"Excel saved to {file_path}")
