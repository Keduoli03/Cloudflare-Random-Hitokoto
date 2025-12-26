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
OUTPUT_DIR = Path("orig_data")
CATEGORIES_DIR = Path("categories")

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

SHARD_DEPTH = 0

def generate_cf_rule(is_category: bool, hex_len: int, shard_depth: int, categories: list = None) -> str:
    """生成 Cloudflare 规则表达式"""
    # 路径前缀
    base_dir = "/categories/" if is_category else "/orig_data/"
    
    parts = [f'"{base_dir}"']
    
    # 1. 分类参数处理 (仅分类模式)
    if is_category:
        if categories:
            # 排序：长度短的在内层，长度长的在外层（后遍历）
            # 这样 if(long, long, if(short, short, ...))
            # 确保 c=ab 先匹配 ab，而不是先匹配 a (如果 contains c=a 包含 c=ab 的情况)
            categories_sorted = sorted(categories, key=lambda x: (len(x), x))
            
            # 使用嵌套 if 表达式来匹配分类
            # 默认 fallback 到一个不存在的分类名，确保输错参数时返回 404
            fallback = "unknown_category"
            expr = f'"{fallback}"'
            
            for cat in categories_sorted:
                cond = f'http.request.uri.query contains "c={cat}"'
                expr = f'if({cond}, "{cat}", {expr})'
            
            parts.append(expr)
        else:
            # 如果没有分类列表（不应该发生），回退到旧逻辑
            parts.append('substring(http.request.uri.query, 2, 1)')
            
        parts.append('"/"')

    # 2. 文件名
    # 直接使用 uuidv4 截取 hex_len 长度
    parts.append(f'substring(uuidv4(cf.random_seed), 0, {hex_len})')
         
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
    """通用写入函数 (总是使用 Fill-Full 策略)"""
    if not data_list:
        print(f"  [Warning] No data to write for {base_dir}")
        return

    total_slots = 16 ** hex_len
    
    # 总是初始化所有 buckets
    buckets = [[] for _ in range(total_slots)]
    
    # 循环填充所有槽位
    data_cycle = cycle(data_list)
    print(f"  [Fill-Full] Filling {total_slots} slots with {len(data_list)} items...")
    for i in range(total_slots):
        buckets[i].append(next(data_cycle))
    
    # 写入文件
    count = 0
    for i in range(total_slots):
        content = buckets[i]
        if not content:
            continue
            
        hex_str = format(i, f'0{hex_len}x')
        file_path = get_file_path(base_dir, hex_str, shard_depth)
        ensure_dir(file_path.parent)
        
        # 单文件单对象逻辑
        final_data = content[0]
            
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(final_data, f, ensure_ascii=False, separators=(',', ':'))
            
        count += 1
        if count % 10000 == 0:
            print(f"    Written {count}/{total_slots} files...")

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

    if not SOURCE_DIR.exists():
        print(f"Source directory {SOURCE_DIR} does not exist. Please run inside the correct path.")
        return

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
        print("Error: No data found in SOURCE_DIR.")
        print("Please ensure 'sentences-bundle' is cloned and contains .json files.")
        return

    # 1. 生成全量数据
    global_hex_len = calculate_hex_len(len(all_objects), MIN_HEX_LEN)
    
    print(f"\n[Global Data] Items: {len(all_objects)}")
    print(f"  -> Auto-scaled HEX_LEN: {global_hex_len} (Capacity: {16**global_hex_len})")
    print(f"  -> Strategy: Fill-Full (Cycle through data to fill all slots)")
    
    write_files(all_objects, OUTPUT_DIR, global_hex_len, SHARD_DEPTH)
    
    # 2. 生成分类数据
    # 过滤掉空分类
    valid_categories = {k: v for k, v in category_map.items() if v}
    
    max_cat_items = 0
    if valid_categories:
        max_cat_items = max(len(d) for d in valid_categories.values())
    
    cat_hex_len = calculate_hex_len(max_cat_items, min_len=3)
    
    print(f"\n[Category Data] Max Category Items: {max_cat_items}")
    print(f"  -> Auto-scaled HEX_LEN: {cat_hex_len} (Capacity: {16**cat_hex_len})")
    print(f"  -> Strategy: Fill-Full (Cycle through data to fill all slots)")
    
    for cat_name, cat_data in valid_categories.items():
        cat_dir = CATEGORIES_DIR / cat_name
        ensure_dir(cat_dir)
        write_files(cat_data, cat_dir, cat_hex_len, SHARD_DEPTH)

    # 3. 生成规则文件
    print("\nGenerating rules.txt...")
    rule_global = generate_cf_rule(False, global_hex_len, SHARD_DEPTH)
    rule_category = generate_cf_rule(True, cat_hex_len, SHARD_DEPTH, categories=list(valid_categories.keys()))
    
    with open("rules.txt", "w", encoding="utf-8") as f:
        f.write("=== Cloudflare Transform Rules (Auto Generated) ===\n")
        if not TARGET_DOMAIN:
            f.write("!!! IMPORTANT: Replace 'api.yourdomain.com' with your actual subdomain !!!\n\n")
        else:
            f.write(f"Target Domain: {TARGET_DOMAIN}\n\n")
        
        domain_check = f'(http.host eq "{TARGET_DOMAIN if TARGET_DOMAIN else "api.yourdomain.com"}")'

        f.write(f"[Rule 1: Hitokoto Random] (HEX_LEN={global_hex_len}, SHARD={SHARD_DEPTH})\n")
        f.write(f"Condition: {domain_check} and (http.request.uri.path eq \"/\") and (not http.request.uri.query contains \"c=\")\n")
        f.write("Expression:\n")
        f.write(rule_global + "\n\n")
        
        f.write("-" * 50 + "\n\n")
        
        f.write(f"[Rule 2: Hitokoto Category] (HEX_LEN={cat_hex_len}, SHARD={SHARD_DEPTH})\n")
        f.write(f"Condition: {domain_check} and (http.request.uri.path eq \"/\") and (http.request.uri.query contains \"c=\")\n")
        f.write("Expression:\n")
        f.write(rule_category + "\n")
        
    print(f"Done. Please check 'rules.txt' for the latest Cloudflare rules.")

if __name__ == "__main__":
    main()
