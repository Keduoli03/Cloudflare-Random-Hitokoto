import json
from pathlib import Path
from itertools import cycle
import shutil

import math

# ================= 配置区域 =================
# 基础 Hex 长度 (保底)
# 4 -> 65,536 个文件
MIN_HEX_LEN = 4

# 目录分层深度
SHARD_DEPTH = 2

# 每个文件是否存储多条数据
STORE_AS_LIST = False

SOURCE_DIR = Path("sentences-bundle/sentences")
OUTPUT_DIR = Path("advanced_data")
CATEGORIES_DIR = Path("advanced_categories")
# ===========================================

def calculate_hex_len(item_count: int, min_len: int) -> int:
    """根据数据量自动计算所需的 Hex 长度"""
    if item_count == 0:
        return min_len
    # 计算需要的位数: log16(count)
    # 例如 count=65537 -> log16=4.00001 -> ceil=5
    needed = math.ceil(math.log(item_count, 16))
    return max(min_len, needed)

def generate_cf_rule(is_category: bool, hex_len: int, shard_depth: int) -> str:
    """生成 Cloudflare 规则表达式"""
    # 路径前缀
    base_dir = "/advanced_categories/" if is_category else "/advanced_data/"
    
    parts = [f'"{base_dir}"']
    
    # 1. 分类参数处理 (仅分类模式)
    if is_category:
        # 假设 c=a, 取第2位开始的1个字符
        parts.append('substring(http.request.uri.query, 2, 1)')
        parts.append('"/"')

    # 2. 目录分层 (Shard)
    # 例如 shard_depth=2, 意味着前2位 hex 用于目录
    # substring(uuid, 0, 1) -> 第1层
    # substring(uuid, 1, 1) -> 第2层
    for i in range(shard_depth):
        parts.append(f'substring(uuidv4(cf.random_seed), {i}, 1)')
        parts.append('"/"')
    
    # 3. 文件名 (剩余的所有 hex)
    # 比如 hex_len=4, shard=2, 剩下 2 位 (index 2, length 2)
    filename_len = hex_len - shard_depth
    parts.append(f'substring(uuidv4(cf.random_seed), {shard_depth}, {filename_len})')
    parts.append('".json"')
    
    return f"concat({', '.join(parts)})"

def ensure_dir(path: Path):
    if not path.exists():
        path.mkdir(parents=True)

def get_file_path(base_dir: Path, hex_str: str) -> Path:
    """根据 hex 字符串和分层配置生成文件路径"""
    if SHARD_DEPTH == 0:
        return base_dir / f"{hex_str}.json"
    
    parts = []
    # 构建目录部分
    for i in range(SHARD_DEPTH):
        parts.append(hex_str[i])
    
    # 构建文件名部分 (剩余的 hex)
    filename = f"{hex_str[SHARD_DEPTH:]}.json"
    
    # 组合
    return base_dir / "/".join(parts) / filename

def write_files(data_list, base_dir: Path, hex_len: int):
    """通用写入函数"""
    total_slots = 16 ** hex_len
    buckets = [[] for _ in range(total_slots)]
    
    data_cycle = cycle(data_list)
    
    print(f"Distributing {len(data_list)} items into {total_slots} buckets...")
    for i in range(total_slots):
        obj = next(data_cycle)
        buckets[i].append(obj)
        
    print(f"Writing files to {base_dir}...")
    for i in range(total_slots):
        hex_str = format(i, f'0{hex_len}x')
        file_path = get_file_path(base_dir, hex_str)
        ensure_dir(file_path.parent)
        
        content = buckets[i]
        # 单文件单对象逻辑
        final_data = content
        if not STORE_AS_LIST and len(content) == 1:
            final_data = content[0]
            
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(final_data, f, ensure_ascii=False, separators=(',', ':'))
            
        if i % 5000 == 0 and i > 0:
            print(f"  Progress: {i}/{total_slots}")

def main():
    # 清理旧数据
    if OUTPUT_DIR.exists():
        shutil.rmtree(OUTPUT_DIR)
    if CATEGORIES_DIR.exists():
        shutil.rmtree(CATEGORIES_DIR)
        
    ensure_dir(OUTPUT_DIR)
    ensure_dir(CATEGORIES_DIR)

    print("Loading data...")
    all_objects = []
    category_map = {} # key: category_name, value: list of objects

    for file_path in SOURCE_DIR.glob("*.json"):
        category_name = file_path.stem # e.g. 'a', 'b'
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
                if isinstance(data, list):
                    all_objects.extend(data)
                    # 收集分类数据
                    if category_name not in category_map:
                        category_map[category_name] = []
                    category_map[category_name].extend(data)
            except Exception as e:
                print(f"Error reading {file_path}: {e}")

    if not all_objects:
        print("No data found.")
        return

    # 1. 生成全量数据 (HEX_LEN)
    print(f"\n[Global Data] Configuration: HEX_LEN={HEX_LEN}")
    write_files(all_objects, OUTPUT_DIR, HEX_LEN)
    
    # 2. 生成分类数据
    # 分类数据量通常较小，我们可以用稍小的 HEX_LEN，或者保持一致
    # 为了保证路径规则统一，建议保持一致，或者根据数据量动态调整。
    # 考虑到 GitHub Pages 没限制，且为了规则统一，我们对分类也使用相同的 HEX_LEN - 1 (或者自定义)
    # 原项目分类是 4096 (16^3)，全量是 65536 (16^4)。
    # 这里我们设定分类为 HEX_LEN - 1，如果 HEX_LEN=4，则分类为 3 (4096个文件)，足够了。
    CAT_HEX_LEN = max(3, HEX_LEN - 1)
    
    print(f"\n[Category Data] Configuration: HEX_LEN={CAT_HEX_LEN}")
    for cat_name, cat_data in category_map.items():
        print(f"Processing category: {cat_name} ({len(cat_data)} items)")
        cat_dir = CATEGORIES_DIR / cat_name
        ensure_dir(cat_dir)
        write_files(cat_data, cat_dir, CAT_HEX_LEN)

    print("\nAll Done.")

if __name__ == "__main__":
    main()
