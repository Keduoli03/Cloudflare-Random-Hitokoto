import json
from pathlib import Path
from itertools import cycle
import shutil

import math

# ================= 配置区域 =================
# 基础 Hex 长度 (保底)
# 4 -> 65,536 个文件
MIN_HEX_LEN = 4

# 每个文件是否存储多条数据
STORE_AS_LIST = False

SOURCE_DIR = Path("sentences-bundle/sentences")
OUTPUT_DIR = Path("advanced_data")
CATEGORIES_DIR = Path("advanced_categories")

# 你的 API 域名 (生成规则时会用到)
# 留空则生成通用规则模板
TARGET_DOMAIN = "hitokoto.blueke.dpdns.org"

# ===========================================

def calculate_hex_len(item_count: int, min_len: int) -> int:
    """根据数据量自动计算所需的 Hex 长度"""
    if item_count == 0:
        return min_len
    # 计算需要的位数: log16(count)
    # 例如 count=65537 -> log16=4.00001 -> ceil=5
    needed = math.ceil(math.log(item_count, 16))
    return max(min_len, needed)

def calculate_shard_depth(hex_len: int) -> int:
    """
    智能计算目录分层深度 (Smart Sharding)
    目标：确保每个目录下文件数量不超过阈值 (如 4096)，避免单个目录过大。
    公式：Shard Depth = max(0, hex_len - 3)
    
    hex_len=3 (4096) -> depth=0 -> 4096 files/dir (ok)
    hex_len=4 (65536) -> depth=1 -> 16 dirs * 4096 files (ok)
    hex_len=5 (1M) -> depth=2 -> 256 dirs * 4096 files (ok)
    """
    return max(0, hex_len - 3)

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
    # 为了绕过 Cloudflare 免费版 uuidv4() 只能调用 1 次的限制
    # 我们使用 http.request.id (也是 hex 字符串) 来生成目录层级的随机数
    for i in range(shard_depth):
        parts.append(f'substring(http.request.id, {i}, 1)')
        parts.append('"/"')
    
    # 3. 文件名 (剩余的所有 hex)
    # 文件名部分使用 uuidv4，保证唯一性和随机性
    # 比如 hex_len=4, shard=2, 剩下 2 位
    filename_len = hex_len - shard_depth
    # 注意：这里从 0 开始截取即可，因为 uuidv4 是独立的随机源，不需要接着 request.id 后面
    parts.append(f'substring(uuidv4(cf.random_seed), 0, {filename_len})')
    parts.append('".json"')
    
    return f"concat({', '.join(parts)})"

def ensure_dir(path: Path):
    if not path.exists():
        path.mkdir(parents=True)

def get_file_path(base_dir: Path, hex_str: str, shard_depth: int) -> Path:
    """根据 hex 字符串和分层配置生成文件路径"""
    if shard_depth == 0:
        return base_dir / f"{hex_str}.json"
    
    parts = []
    # 构建目录部分
    for i in range(shard_depth):
        parts.append(hex_str[i])
    
    # 构建文件名部分 (剩余的 hex)
    filename = f"{hex_str[shard_depth:]}.json"
    
    # 组合
    return base_dir / "/".join(parts) / filename

def write_files(data_list, base_dir: Path, hex_len: int, shard_depth: int):
    """通用写入函数"""
    total_slots = 16 ** hex_len
    buckets = [[] for _ in range(total_slots)]
    
    data_cycle = cycle(data_list)
    
    print(f"Distributing {len(data_list)} items into {total_slots} buckets (Depth: {shard_depth})...")
    for i in range(total_slots):
        obj = next(data_cycle)
        buckets[i].append(obj)
        
    print(f"Writing files to {base_dir}...")
    for i in range(total_slots):
        hex_str = format(i, f'0{hex_len}x')
        file_path = get_file_path(base_dir, hex_str, shard_depth)
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

    # 1. 生成全量数据
    # 自动计算 HEX_LEN
    global_hex_len = calculate_hex_len(len(all_objects), MIN_HEX_LEN)
    global_shard_depth = calculate_shard_depth(global_hex_len)
    
    print(f"\n[Global Data] Items: {len(all_objects)}")
    print(f"  -> Auto-scaled HEX_LEN: {global_hex_len} (Capacity: {16**global_hex_len})")
    print(f"  -> Smart Shard Depth: {global_shard_depth}")
    
    write_files(all_objects, OUTPUT_DIR, global_hex_len, global_shard_depth)
    
    # 2. 生成分类数据
    # 计算所有分类中最大的需求
    max_cat_items = 0
    if category_map:
        max_cat_items = max(len(d) for d in category_map.values())
    
    # 分类数据通常比全量少，但为了安全，我们也自动计算
    # 比如分类里有 7万条数据，也得升到 Level 5
    # 我们设定分类的最小长度为 3 (4096)，稍微小一点以节省文件数
    cat_hex_len = calculate_hex_len(max_cat_items, min_len=3)
    cat_shard_depth = calculate_shard_depth(cat_hex_len)
    
    print(f"\n[Category Data] Max Category Items: {max_cat_items}")
    print(f"  -> Auto-scaled HEX_LEN: {cat_hex_len} (Capacity: {16**cat_hex_len})")
    print(f"  -> Smart Shard Depth: {cat_shard_depth}")
    
    for cat_name, cat_data in category_map.items():
        # print(f"Processing category: {cat_name} ({len(cat_data)} items)")
        cat_dir = CATEGORIES_DIR / cat_name
        ensure_dir(cat_dir)
        write_files(cat_data, cat_dir, cat_hex_len, cat_shard_depth)

    # 3. 生成规则文件
    print("\nGenerating rules.txt...")
    rule_global = generate_cf_rule(False, global_hex_len, global_shard_depth)
    rule_category = generate_cf_rule(True, cat_hex_len, cat_shard_depth)
    
    with open("rules.txt", "w", encoding="utf-8") as f:
        f.write("=== Cloudflare Transform Rules (Auto Generated) ===\n")
        if not TARGET_DOMAIN:
            f.write("!!! IMPORTANT: Replace 'api.yourdomain.com' with your actual subdomain !!!\n\n")
        else:
            f.write(f"Target Domain: {TARGET_DOMAIN}\n\n")
        
        domain_check = f'(http.host eq "{TARGET_DOMAIN if TARGET_DOMAIN else "api.yourdomain.com"}")'

        f.write(f"[Rule 1: Hitokoto Random] (HEX_LEN={global_hex_len}, SHARD={global_shard_depth})\n")
        f.write(f"Condition: {domain_check} and (http.request.uri.path eq \"/\") and (not http.request.uri.query contains \"c=\")\n")
        f.write("Expression:\n")
        f.write(rule_global + "\n\n")
        
        f.write("-" * 50 + "\n\n")
        
        f.write(f"[Rule 2: Hitokoto Category] (HEX_LEN={cat_hex_len}, SHARD={cat_shard_depth})\n")
        f.write(f"Condition: {domain_check} and (http.request.uri.path eq \"/\") and (http.request.uri.query contains \"c=\")\n")
        f.write("Expression:\n")
        f.write(rule_category + "\n")
        
    print(f"Done. Please check 'rules.txt' for the latest Cloudflare rules.")

if __name__ == "__main__":
    main()
