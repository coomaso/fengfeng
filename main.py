import requests
import base64
import json
import time
import random
import os
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple, Optional, Any, Union
from urllib.parse import quote
from Crypto.Cipher import AES
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, Border, Side, PatternFill
from openpyxl.utils import get_column_letter

# ==================== 配置模块 ====================
class Config:
    """全局配置参数"""
    # 请求配置
    RETRY_COUNT = 3
    PAGE_RETRY_MAX = 3
    TIMEOUT = 15
    PAGE_SIZE = 10
    MAX_DETAIL_THREADS = 2  # 并发获取明细的最大线程数
    DETAIL_RETRY = 3
    DETAIL_DELAY = (10, 30)  # 随机延迟范围（秒）

    # AES 配置（可通过环境变量覆盖）
    AES_KEY = os.getenv("AES_KEY", "6875616E6779696E6875616E6779696E").encode()
    AES_IV = os.getenv("AES_IV", "sskjKingFree5138").encode()

    # 基础URL
    BASE_URL = "http://106.15.60.27:22222"
    CODE_URL = f"{BASE_URL}/ycdc/bakCmisYcOrgan/getCreateCode"
    PAGE_URL = f"{BASE_URL}/ycdc/bakCmisYcOrgan/getCurrentIntegrityPage"
    DETAIL_URL = f"{BASE_URL}/ycdc/bakCmisYcOrgan/getCurrentIntegrityDetails"

    # 请求头（固定）
    HEADERS = {
        "Accept": "application/json",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,vi;q=0.7",
        "Connection": "keep-alive",
        "Content-Type": "application/json; charset=utf-8",
        "Host": "106.15.60.27:22222",
        "Referer": "http://106.15.60.27:22222/xxgs/",
        "Sec-Ch-Ua": '"Chromium";v="122", "Not(A:Brand";v="24", "Google Chrome";v="122"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.6261.95 Safari/537.36"
    }

    # Excel列配置
    COLUMNS = [
        {'id': 'cioName', 'name': '企业名称', 'width': 35, 'merge': True, 'align': 'left'},
        {'id': 'eqtName', 'name': '资质类别', 'width': 20, 'merge': True, 'align': 'center'},
        {'id': 'csf', 'name': '初始分', 'width': 12, 'merge': True, 'align': 'center', 'format': '0'},
        {'id': 'zzmx', 'name': '资质明细', 'width': 50, 'merge': False, 'align': 'left'},
        {'id': 'cxdj', 'name': '诚信等级', 'width': 12, 'merge': False, 'align': 'center'},
        {'id': 'score', 'name': '诚信分值', 'width': 12, 'merge': False, 'align': 'center', 'format': '0.00'},
        {'id': 'jcf', 'name': '基础分', 'width': 12, 'merge': False, 'align': 'center', 'format': '0'},
        {'id': 'zxjf', 'name': '专项加分', 'width': 12, 'merge': False, 'align': 'center', 'format': '0.00'},
        {'id': 'kf', 'name': '扣分', 'width': 12, 'merge': False, 'align': 'center', 'format': '0.00'},
        {'id': 'eqlId', 'name': '资质ID', 'width': 25, 'merge': False, 'align': 'center'},
        {'id': 'orgId', 'name': '组织ID', 'width': 30, 'merge': True, 'align': 'center'},
        {'id': 'cecId', 'name': '信用档案ID', 'width': 30, 'merge': True, 'align': 'center'}
    ]

    # 工作表配置
    SHEET_CONFIGS = [
        {"name": "企业信用数据汇总", "prefix": None, "freeze": 'B2', "merge": True},
        {"name": "建筑工程总承包信用分排序", "prefix": "建筑业企业资质_施工总承包_建筑工程_", "freeze": 'B2', "merge": False, "generate_json": True},
        {"name": "市政公用工程信用分排序", "prefix": "建筑业企业资质_施工总承包_市政公用工程_", "freeze": 'B2', "merge": False, "generate_json": True},
        {"name": "装修装饰工程信用分排序", "prefix": "建筑业企业资质_专业承包_建筑装修装饰工程_", "freeze": 'B2', "merge": False, "generate_json": True},
        {"name": "水利水电工程信用分排序", "prefix": "建筑业企业资质_施工总承包_水利水电工程_", "freeze": 'B2', "merge": False, "generate_json": True},
        {"name": "电力工程信用分排序", "prefix": "建筑业企业资质_施工总承包_电力工程_", "freeze": 'B2', "merge": False, "generate_json": True}
    ]

# ==================== 日志配置 ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# ==================== 辅助函数 ====================
def get_beijing_time() -> datetime:
    """获取当前北京时间（UTC+8）"""
    return datetime.now(timezone(timedelta(hours=8)))

def safe_request(session: requests.Session, url: str, params: Optional[Dict] = None) -> requests.Response:
    """带指数退避重试的请求"""
    for attempt in range(1, Config.RETRY_COUNT + 1):
        try:
            if attempt > 1:
                delay = 2 ** (attempt - 1) + random.uniform(0, 1)  # 指数退避
                logger.debug(f"请求 {url} 重试 {attempt}/{Config.RETRY_COUNT}，等待 {delay:.2f} 秒")
                time.sleep(delay)
            resp = session.get(url, headers=Config.HEADERS, params=params, timeout=Config.TIMEOUT)
            resp.raise_for_status()
            return resp
        except requests.exceptions.Timeout:
            logger.warning(f"请求超时: {url} (尝试 {attempt}/{Config.RETRY_COUNT})")
        except requests.exceptions.RequestException as e:
            logger.warning(f"请求异常: {url} - {e} (尝试 {attempt}/{Config.RETRY_COUNT})")
    raise RuntimeError(f"请求失败超过最大重试次数: {url}")

def aes_decrypt_base64(encrypted_base64: str) -> str:
    """AES-CBC解密，返回UTF-8字符串"""
    if not encrypted_base64:
        raise ValueError("加密数据为空")
    try:
        encrypted_bytes = base64.b64decode(encrypted_base64)
        cipher = AES.new(Config.AES_KEY, AES.MODE_CBC, Config.AES_IV)
        decrypted = cipher.decrypt(encrypted_bytes).rstrip(b'\x00')
        return decrypted.decode("utf-8")
    except Exception as e:
        logger.error(f"AES解密失败: {e}")
        raise

def parse_encrypted_response(encrypted_data: str) -> Dict[str, Any]:
    """解密并解析JSON数据，返回字典"""
    if not encrypted_data:
        logger.warning("收到空的加密数据")
        return {"error": "empty data"}
    try:
        decrypted_str = aes_decrypt_base64(encrypted_data)
        return json.loads(decrypted_str)
    except json.JSONDecodeError as e:
        logger.error(f"JSON解析失败: {e}，数据片段: {encrypted_data[:200]}")
        return {"error": f"invalid json: {e}"}
    except Exception as e:
        logger.error(f"解析异常: {e}")
        return {"error": str(e)}

def get_new_code(session: requests.Session) -> Tuple[str, str]:
    """获取验证码和时间戳"""
    timestamp = str(int(time.time() * 1000))
    url = f"{Config.CODE_URL}?codeValue={timestamp}"
    try:
        resp = safe_request(session, url)
        data = resp.json()
        if data.get("code") != 0:
            raise RuntimeError(f"验证码接口异常: {data}")
        code = aes_decrypt_base64(data["data"])
        logger.info(f"获取验证码成功: {code[:4]}... 时间戳: {timestamp}")
        return code, timestamp
    except Exception as e:
        logger.error(f"获取验证码失败: {e}")
        raise

def fetch_page(session: requests.Session, page: int, code: str, timestamp: str) -> Tuple[List[Dict], int]:
    """获取单页数据，返回(记录列表, 总记录数)"""
    url = (
        f"{Config.PAGE_URL}?pageSize={Config.PAGE_SIZE}"
        f"&cioName=%E5%85%AC%E5%8F%B8&page={page}"
        f"&code={quote(code)}&codeValue={timestamp}"
    )
    for attempt in range(Config.PAGE_RETRY_MAX + 1):
        try:
            resp = safe_request(session, url)
            page_data = resp.json()
            if page_data.get("code") != 0:
                logger.warning(f"第 {page} 页返回code={page_data.get('code')}，尝试重试")
                if attempt < Config.PAGE_RETRY_MAX:
                    time.sleep(random.uniform(1, 3))
                    continue
                raise RuntimeError(f"接口返回错误码: {page_data}")

            encrypted = page_data.get("data")
            if not encrypted:
                logger.warning(f"第 {page} 页data为空，尝试重试")
                if attempt < Config.PAGE_RETRY_MAX:
                    time.sleep(random.uniform(1, 3))
                    continue
                raise RuntimeError("连续空数据响应")

            parsed = parse_encrypted_response(encrypted)
            records = parsed.get("data", [])
            total = parsed.get("total", 0)
            logger.debug(f"第 {page} 页获取到 {len(records)} 条记录")
            return records, total
        except Exception as e:
            logger.error(f"第 {page} 页处理失败 (尝试 {attempt+1}): {e}")
            if attempt == Config.PAGE_RETRY_MAX:
                raise
            time.sleep(random.uniform(1, 3))
    raise RuntimeError("无法获取页面数据")

def fetch_company_detail(session: requests.Session, cec_id: str, company_name: str) -> Optional[Dict]:
    """获取单个企业明细（带重试）"""
    url = f"{Config.DETAIL_URL}?cecId={cec_id}"
    for attempt in range(1, Config.DETAIL_RETRY + 1):
        try:
            resp = safe_request(session, url)
            data = resp.json()
            if data.get("code") != 0:
                logger.warning(f"明细接口返回错误码: {data}")
                if attempt < Config.DETAIL_RETRY:
                    time.sleep(random.uniform(*Config.DETAIL_DELAY))
                    continue
                return None

            encrypted = data.get("data", "")
            if not encrypted:
                logger.warning(f"明细接口返回空数据 (cecId={cec_id})")
                return None

            parsed = parse_encrypted_response(encrypted)
            detail = parsed.get("data", {})
            return {
                "cioName": detail.get("cioName", company_name),
                "jfsj": detail.get("jfsj", ""),
                "eqtName": detail.get("eqtName", ""),
                "blxwArray": detail.get("blxwArray", []),
                "lhxwArray": detail.get("lhxwArray", []),
                "cecId": detail.get("cecId", cec_id),
                "cechId": detail.get("cechId", "")
            }
        except Exception as e:
            logger.error(f"获取明细失败 (cecId={cec_id}, 尝试 {attempt}/{Config.DETAIL_RETRY}): {e}")
            if attempt < Config.DETAIL_RETRY:
                time.sleep(random.uniform(*Config.DETAIL_DELAY))
    return None

def fetch_details_concurrent(session: requests.Session, companies: List[Dict]) -> Dict[str, Dict]:
    """并发获取企业明细，返回cecId->detail的映射"""
    details = {}
    with ThreadPoolExecutor(max_workers=Config.MAX_DETAIL_THREADS) as executor:
        future_to_cec = {
            executor.submit(fetch_company_detail, session, item["cecId"], item["cioName"]): item["cecId"]
            for item in companies if item.get("cecId")
        }
        for future in as_completed(future_to_cec):
            cec_id = future_to_cec[future]
            try:
                detail = future.result()
                if detail:
                    details[cec_id] = detail
                else:
                    logger.warning(f"未能获取明细: cecId={cec_id}")
            except Exception as e:
                logger.error(f"获取明细异常 cecId={cec_id}: {e}")
    return details

def append_top_json(sorted_data: List[Dict], category_name: str, output_dir: Path) -> Optional[Path]:
    """追加数据到当天JSON文件"""
    now = get_beijing_time()
    date_str = now.strftime("%Y%m%d")
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    json_path = output_dir / f"{category_name}_top10.json"

    # 准备本次数据
    data_list = []
    for idx, item in enumerate(sorted_data[:10], 1):
        company_data = {
            "排名": idx,
            "企业名称": item.get("cioName", ""),
            "诚信分值": item.get("score", 0),
            "组织ID": item.get("orgId", ""),
        }
        if "detail" in item:
            company_data["信誉分明细"] = item["detail"]
        data_list.append(company_data)

    update_data = {"TIMEamp": timestamp, "DATAlist": data_list}

    # 读取或初始化
    existing = []
    if json_path.exists():
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                existing = json.load(f)
            if not isinstance(existing, list):
                existing = [existing]
        except:
            existing = []
    existing.append(update_data)

    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)
    logger.info(f"JSON已更新: {json_path}")
    return json_path

def process_raw_data(all_data: List[Dict]) -> List[Dict]:
    """将原始数据转换为Excel所需格式（扁平化）"""
    processed = []
    for item in all_data:
        if not isinstance(item, dict):
            continue
        if item.get('eqtName') != '施工':
            continue

        main = {
            'cioName': item.get('cioName', ''),
            'eqtName': item.get('eqtName', ''),
            'csf': float(item.get('csf', 0)),
            'orgId': item.get('orgId', ''),
            'cecId': item.get('cecId', '')
        }

        details = item.get('zzmxcxfArray', [])
        if not details:
            # 无明细则插入一条空记录
            processed.append({**main, 'zzmx': '', 'cxdj': '', 'score': 0, 'jcf': 0, 'zxjf': 0, 'kf': 0, 'eqlId': ''})
        else:
            for d in details:
                processed.append({
                    **main,
                    'zzmx': d.get('zzmx', ''),
                    'cxdj': d.get('cxdj', ''),
                    'score': float(d.get('score', 0)),
                    'jcf': float(d.get('jcf', 0)),
                    'zxjf': float(d.get('zxjf', 0)),
                    'kf': float(d.get('kf', 0)),
                    'eqlId': d.get('eqlId', '')
                })
    return processed

def export_to_excel(processed_data: List[Dict], details_cache: Dict[str, Dict], output_dir: Path, timestamp: str) -> Path:
    """生成主Excel文件，返回文件路径"""
    wb = Workbook()
    # 创建所有工作表
    sheets = {}
    for cfg in Config.SHEET_CONFIGS:
        if cfg["name"] == "企业信用数据汇总":
            ws = wb.active
            ws.title = cfg["name"]
        else:
            ws = wb.create_sheet(cfg["name"])
        sheets[cfg["name"]] = ws
        ws.freeze_panes = cfg["freeze"]

    # 写入表头样式
    header_fill = PatternFill("solid", fgColor="003366")
    header_font = Font(bold=True, color="FFFFFF")
    header_alignment = Alignment(horizontal="center", vertical="center")
    header_border = Border(left=Side(style="thin"), right=Side(style="thin"), top=Side(style="thin"), bottom=Side(style="thin"))

    for ws in sheets.values():
        ws.append([col['name'] for col in Config.COLUMNS])
        for idx, col in enumerate(Config.COLUMNS, 1):
            cell = ws.cell(row=1, column=idx)
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = header_alignment
            cell.border = header_border
            ws.column_dimensions[get_column_letter(idx)].width = col['width']

    # 写入汇总表数据
    summary_ws = sheets["企业信用数据汇总"]
    merge_map = {}
    current_key = None
    start_row = 2

    for row_idx, row_data in enumerate(processed_data, 2):
        unique_key = f"{row_data['orgId']}-{row_data['cecId']}"
        if unique_key != current_key:
            if current_key is not None:
                merge_map[current_key] = (start_row, row_idx - 1)
            current_key = unique_key
            start_row = row_idx

        summary_ws.append([row_data.get(col['id'], '') for col in Config.COLUMNS])
        for col_idx in range(1, len(Config.COLUMNS) + 1):
            cell = summary_ws.cell(row=row_idx, column=col_idx)
            cell.border = Border(left=Side(style="thin"), right=Side(style="thin"), top=Side(style="thin"), bottom=Side(style="thin"))
            col_def = Config.COLUMNS[col_idx - 1]
            cell.alignment = Alignment(horizontal=col_def['align'], vertical='center')
            if col_def.get('format'):
                cell.number_format = col_def['format']

    # 合并单元格
    for col in Config.COLUMNS:
        if col['merge']:
            col_letter = get_column_letter(Config.COLUMNS.index(col) + 1)
            for (start, end) in merge_map.values():
                if end > start:
                    summary_ws.merge_cells(f"{col_letter}{start}:{col_letter}{end}")

    # 处理其他工作表
    json_files = []
    for cfg in Config.SHEET_CONFIGS[1:]:  # 跳过汇总表
        ws = sheets[cfg["name"]]
        # 筛选数据
        sheet_data = [
            d for d in processed_data
            if str(d.get('zzmx', '')).startswith(cfg["prefix"]) and '级' in str(d.get('zzmx', ''))
        ]
        sheet_data.sort(key=lambda x: x.get('score', 0), reverse=True)

        # 获取前10名明细（使用缓存）
        for item in sheet_data[:10]:
            cec_id = item.get('cecId')
            if cec_id and cec_id in details_cache:
                item['detail'] = details_cache[cec_id]

        # 生成JSON
        if cfg.get("generate_json") and sheet_data:
            json_path = append_top_json(sheet_data, cfg["name"], output_dir)
            if json_path:
                json_files.append(str(json_path))

        # 写入数据
        for row_data in sheet_data:
            ws.append([row_data.get(col['id'], '') for col in Config.COLUMNS])
        # 设置数据样式（简化，与汇总表类似）
        for row in ws.iter_rows(min_row=2):
            for cell in row:
                cell.border = Border(left=Side(style="thin"), right=Side(style="thin"), top=Side(style="thin"), bottom=Side(style="thin"))
                col_def = Config.COLUMNS[cell.column - 1]
                cell.alignment = Alignment(horizontal=col_def['align'], vertical='center')
                if col_def.get('format'):
                    cell.number_format = col_def['format']

    # 保存文件
    excel_path = output_dir / f"宜昌市信用评价信息_{timestamp}.xlsx"
    wb.save(excel_path)
    logger.info(f"Excel已保存: {excel_path}")
    return excel_path, json_files

def export_detail_sheet(details_cache: Dict[str, Dict], qual_scores: Dict[str, Dict], output_dir: Path, timestamp: str) -> Optional[Path]:
    """生成信誉分明细表（不良/良好行为）"""
    if not details_cache:
        logger.info("无企业明细数据，跳过信誉分明细表生成")
        return None

    # 构建企业名称映射
    name_map = {}
    for cec_id, detail in details_cache.items():
        name_map[cec_id] = detail.get('cioName', '')

    wb = Workbook()
    wb.remove(wb.active)  # 删除默认工作表
    bad_sheet = wb.create_sheet("不良行为")
    good_sheet = wb.create_sheet("良好行为")

    bad_headers = ["企业名称", "诚信分值", "违规人员", "身份证号", "违规事由", "项目名称",
                   "资质类型", "行为类别", "开始日期", "结束日期", "有效期 (月)", "扣分值", "确认书编号"]
    good_headers = ["企业名称", "诚信分值", "获奖/表彰事由", "项目名称",
                    "资质类型", "行为类别", "开始日期", "结束日期", "有效期 (月)", "加分值", "文号"]
    bad_sheet.append(bad_headers)
    good_sheet.append(good_headers)

    for cec_id, detail in details_cache.items():
        if cec_id not in qual_scores:
            continue
        company_name = name_map.get(cec_id, '')
        for bl in detail.get('blxwArray', []):
            qual_type = bl.get('kfqyzz', '')
            if not qual_type:
                continue
            # 匹配资质类型
            matched_score = None
            for q_name, q_score in qual_scores[cec_id].items():
                if qual_type in q_name or q_name in qual_type:
                    matched_score = q_score
                    break
            if matched_score is None:
                continue
            row = [
                company_name, matched_score,
                bl.get('cfry', ''), bl.get('cfryCertNum', ''),
                bl.get('reason', ''), bl.get('engName', ''),
                qual_type, bl.get('bzXwlb', ''),
                bl.get('beginDate', ''), bl.get('endDate', ''),
                bl.get('valid', ''), bl.get('realValue', 0),
                bl.get('kftzsbh', '')
            ]
            bad_sheet.append(row)

        for lh in detail.get('lhxwArray', []):
            qual_type = lh.get('jfqyzz', '')
            if not qual_type:
                continue
            matched_score = None
            for q_name, q_score in qual_scores[cec_id].items():
                if qual_type in q_name or q_name in qual_type:
                    matched_score = q_score
                    break
            if matched_score is None:
                continue
            proj_name = lh.get('engName', '') or lh.get('hjyy', '')
            row = [
                company_name, matched_score,
                lh.get('reason', ''), proj_name,
                qual_type, lh.get('bzXwlb', ''),
                lh.get('beginDate', ''), lh.get('endDate', ''),
                lh.get('valid', ''), lh.get('realValue', 0),
                lh.get('documentNumber', '')
            ]
            good_sheet.append(row)

    # 设置样式
    for sheet in [bad_sheet, good_sheet]:
        sheet.freeze_panes = 'A2'
        # 表头样式
        header_fill = PatternFill("solid", fgColor="003366")
        header_font = Font(bold=True, color="FFFFFF")
        for cell in sheet[1]:
            cell.fill = header_fill
            cell.font = header_font
            cell.alignment = Alignment(horizontal="center", vertical="center")
            cell.border = Border(left=Side(style="thin"), right=Side(style="thin"), top=Side(style="thin"), bottom=Side(style="thin"))
        # 数据样式
        for row in sheet.iter_rows(min_row=2):
            for cell in row:
                cell.alignment = Alignment(horizontal="center", vertical="center")
                cell.border = Border(left=Side(style="thin"), right=Side(style="thin"), top=Side(style="thin"), bottom=Side(style="thin"))
        # 列宽自适应
        for col in sheet.columns:
            max_len = 0
            col_letter = get_column_letter(col[0].column)
            for cell in col:
                if cell.value:
                    content = str(cell.value)
                    length = sum(2 if '\u4e00' <= c <= '\u9fff' else 1 for c in content)
                    max_len = max(max_len, length)
            adjusted = min(max(max_len + 2, 8), 50)
            sheet.column_dimensions[col_letter].width = adjusted

    detail_path = output_dir / f"信誉分明细表_{timestamp}.xlsx"
    wb.save(detail_path)
    logger.info(f"信誉分明细表已保存: {detail_path}")
    return detail_path

def main():
    logger.info("=== 启动数据获取程序 ===")
    session = requests.Session()
    all_data = []

    try:
        # 获取验证码
        code, ts = get_new_code(session)

        # 获取第一页确定总数
        first_page_data, total = fetch_page(session, 1, code, ts)
        total_pages = (total + Config.PAGE_SIZE - 1) // Config.PAGE_SIZE
        logger.info(f"总记录数: {total}，总页数: {total_pages}")
        all_data.extend(first_page_data)

        # 循环处理剩余页
        for page in range(2, total_pages + 1):
            retry = 0
            while retry <= Config.PAGE_RETRY_MAX:
                try:
                    page_data, _ = fetch_page(session, page, code, ts)
                    all_data.extend(page_data)
                    logger.info(f"第 {page} 页获取成功，当前总记录数: {len(all_data)}")
                    break
                except Exception as e:
                    retry += 1
                    logger.warning(f"第 {page} 页失败 (重试 {retry}/{Config.PAGE_RETRY_MAX}): {e}")
                    if retry > Config.PAGE_RETRY_MAX:
                        logger.error(f"跳过第 {page} 页")
                        break
                    # 刷新验证码
                    code, ts = get_new_code(session)
                    time.sleep(random.uniform(1, 3))

        if not all_data:
            logger.error("未获取到任何数据")
            return

        # 处理原始数据为Excel格式
        processed = process_raw_data(all_data)
        logger.info(f"处理后数据条数: {len(processed)}")

        # 准备输出目录
        output_dir = Path.cwd() / "excel_output"
        output_dir.mkdir(exist_ok=True)
        timestamp = get_beijing_time().strftime("%Y%m%d_%H%M%S")

        # 获取前10名企业（多个工作表需要）
        # 预先获取所有需要明细的企业（前10名在所有工作表中）
        companies_need_detail = set()
        for cfg in Config.SHEET_CONFIGS[1:]:  # 跳过汇总表
            sheet_data = [
                d for d in processed
                if str(d.get('zzmx', '')).startswith(cfg["prefix"]) and '级' in str(d.get('zzmx', ''))
            ]
            sheet_data.sort(key=lambda x: x.get('score', 0), reverse=True)
            for item in sheet_data[:10]:
                if item.get('cecId'):
                    companies_need_detail.add(item['cecId'])

        # 构建需要明细的企业列表
        need_detail = [{'cecId': cid, 'cioName': next((d['cioName'] for d in processed if d['cecId'] == cid), '')}
                       for cid in companies_need_detail]
        details_cache = fetch_details_concurrent(session, need_detail)
        logger.info(f"获取到 {len(details_cache)} 家企业明细")

        # 生成主Excel和JSON
        excel_path, json_files = export_to_excel(processed, details_cache, output_dir, timestamp)

        # 生成信誉分明细表（按资质类型匹配分值≥110）
        # 构建每个企业资质分数映射（仅记录分数≥110的资质）
        qual_scores = {}
        for record in processed:
            cid = record.get('cecId')
            score = record.get('score', 0)
            qual = record.get('zzmx', '')
            if cid and qual:
                if cid not in qual_scores:
                    qual_scores[cid] = {}
                if qual not in qual_scores[cid] or score > qual_scores[cid][qual]:
                    qual_scores[cid][qual] = score
        detail_sheet_path = export_detail_sheet(details_cache, qual_scores, output_dir, timestamp)

        # 设置GitHub Actions输出
        github_output = os.getenv('GITHUB_OUTPUT')
        if github_output:
            with open(github_output, 'a') as f:
                f.write(f"excel-path={excel_path}\n")
                for i, jp in enumerate(json_files, 1):
                    f.write(f"json-path-{i}={jp}\n")
                if detail_sheet_path:
                    f.write(f"detail-sheet-path={detail_sheet_path}\n")

        logger.info("=== 程序执行完成 ===")
    except Exception as e:
        logger.exception("程序执行失败")
        raise
    finally:
        session.close()

if __name__ == "__main__":
    main()
