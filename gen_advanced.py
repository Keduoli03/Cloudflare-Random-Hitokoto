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
    由于 Cloudflare 免费版限制 uuidv4() 只能调用 1 次，
    我们无法既截取目录又截取文件名（因为那是两次调用）。
    
    因此，必须强制使用扁平结构 (Depth=0)，
    所有文件直接存放在 advanced_data/ 下，例如 advanced_data/a1b2.json
    GitHub Pages 对单目录几万个文件支持良好，API 访问不受影响。
    """
    return 0

def generate_cf_rule(is_category: bool, hex_len: int, shard_depth: int, categories: list = None) -> str:
    """生成 Cloudflare 规则表达式"""
    # 路径前缀
    base_dir = "/advanced_categories/" if is_category else "/advanced_data/"
    
    parts = [f'"{base_dir}"']
    
    # 1. 分类参数处理 (仅分类模式)
    if is_category:
        if categories:
            # 排序：长度短的在内层，长度长的在外层（后遍历）
            # 这样 if(long, long, if(short, short, ...))
            # 确保 c=ab 先匹配 ab，而不是先匹配 a (如果 contains c=a 包含 c=ab 的情况)
            # 虽然 Cloudflare contains 可能不区分单词边界，所以这样排序更安全
            categories_sorted = sorted(categories, key=lambda x: (len(x), x))
            
            # 使用嵌套 if 表达式来匹配分类，避免 substring 的不稳定性
            # 默认 fallback 到一个不存在的分类名，确保输错参数时返回 404，而不是错误的分类数据
            fallback = "unknown_category"
            expr = f'"{fallback}"'
            
            for cat in categories_sorted:
                # 简单匹配 c=cat。如果需要更严谨，可以考虑 regex
                # 但免费版对 regex 的支持可能有限制，这里用 contains 足够应对常规 API 调用
                cond = f'http.request.uri.query contains "c={cat}"'
                expr = f'if({cond}, "{cat}", {expr})'
            
            parts.append(expr)
        else:
            # 如果没有分类列表（不应该发生），回退到旧逻辑（虽然不可靠）
            parts.append('substring(http.request.uri.query, 2, 1)')
            
        parts.append('"/"')

    
    # 3. 文件名
    # 直接使用 uuidv4 截取 hex_len 长度
    if is_category:
         # 分类模式下，uuidv4 只用于文件名
         parts.append(f'substring(uuidv4(cf.random_seed), 0, {hex_len})')
    else:
         # 全量模式下，uuidv4 用于整个文件名
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

def write_files(data_list, base_dir: Path, hex_len: int, shard_depth: int, fill_full: bool = False):
    """通用写入函数"""
    total_slots = 16 ** hex_len
    
    # 策略选择:
    # 1. Fill-Full (强制填满): 无论数据多少，循环填充到所有 total_slots
    # 2. Standard (按需填充): 只填充数据量这么多的文件 (已废弃，为了兼容性保留逻辑分支)
    
    # 如果开启 fill_full，我们需要填充所有 buckets
    # 如果关闭，我们只填充 len(data_list) 个 buckets (但这会导致 404，所以建议总是开启或由外部控制)
    
    # 这里我们简化逻辑：总是初始化所有 buckets
    buckets = [[] for _ in range(total_slots)]
    
    if fill_full:
        # 循环填充所有槽位
        data_cycle = cycle(data_list)
        print(f"  [Fill-Full] Filling {total_slots} slots with {len(data_list)} items...")
        for i in range(total_slots):
            buckets[i].append(next(data_cycle))
    else:
        # 按需填充 (旧模式)
        # 注意：现在已经全面采用 Fill-Full，此分支理论上不再执行
        print(f"  [Standard] Filling {len(data_list)} slots...")
        for i, item in enumerate(data_list):
            if i < total_slots:
                buckets[i].append(item)
    
    # 写入文件
    count = 0
    for i in range(total_slots):
        content = buckets[i]
        if not content:
            continue
            
        hex_str = format(i, f'0{hex_len}x')
        file_path = get_file_path(base_dir, hex_str, shard_depth)
        ensure_dir(file_path.parent)
        
        # 单文件单对象逻辑 (始终启用，原作者模式)
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
        print("No data found in SOURCE_DIR.")
        
        # 尝试读取 categories.json 获取分类列表以生成规则
        cat_keys = []
        if Path("categories.json").exists():
            try:
                with open("categories.json", "r", encoding="utf-8") as f:
                    cats = json.load(f)
                    cat_keys = [c['key'] for c in cats]
                    print(f"Loaded {len(cat_keys)} categories from categories.json: {cat_keys}")
            except Exception as e:
                print(f"Failed to load categories.json: {e}")

        if cat_keys:
             print("\nGenerating rules.txt based on categories.json (Simulation Mode)...")
             # 使用默认值，因为没有真实数据
             # 默认用 4位 Hex (65536) 确保和真实环境一致
             global_hex_len = 4 
             cat_hex_len = 3    
             global_shard_depth = 0
             cat_shard_depth = 0
             
             rule_global = generate_cf_rule(False, global_hex_len, global_shard_depth)
             rule_category = generate_cf_rule(True, cat_hex_len, cat_shard_depth, categories=cat_keys)
             
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
             
             print(f"Done. Please check 'rules.txt'.")
             return
             
        return

    # 1. 生成全量数据
    # 自动计算 HEX_LEN
    # 注意：这里我们使用 Fill-Full 策略，所以 HEX_LEN 的计算逻辑需要稍微调整
    # 只要数据量 > 16^(n-1)，我们就应该使用 n 位，并且把这 n 位填满。
    # 例如：60000条 -> 16^4=65536 -> 用 4位，填满 65536 个文件。
    global_hex_len = calculate_hex_len(len(all_objects), MIN_HEX_LEN)
    global_shard_depth = calculate_shard_depth(global_hex_len)
    
    print(f"\n[Global Data] Items: {len(all_objects)}")
    print(f"  -> Auto-scaled HEX_LEN: {global_hex_len} (Capacity: {16**global_hex_len})")
    print(f"  -> Strategy: Fill-Full (Cycle through data to fill all slots)")
    
    write_files(all_objects, OUTPUT_DIR, global_hex_len, global_shard_depth, fill_full=True)
    
    # 2. 生成分类数据
    # 计算所有分类中最大的需求
    max_cat_items = 0
    if category_map:
        max_cat_items = max(len(d) for d in category_map.values())
    
    # 智能扩容策略 (Auto-Scaling + Fill-Full Strategy)
    # 1. 计算足以容纳最大分类的最小 Hex 长度
    #    例如: 4000条 -> log16(4000) ≈ 2.99 -> ceil=3 (容量 4096) -> 够用
    #    例如: 5000条 -> log16(5000) ≈ 3.07 -> ceil=4 (容量 65536) -> 自动扩容到 4位
    # 2. 统一使用这个长度，把所有分类的坑位全部填满 (cycle填充)
    #    这样无论哪个分类，无论随机到哪个 Hex，都保证有文件。
    cat_hex_len = calculate_hex_len(max_cat_items, min_len=3)
    cat_shard_depth = calculate_shard_depth(cat_hex_len)
    
    print(f"\n[Category Data] Max Category Items: {max_cat_items}")
    print(f"  -> Auto-scaled HEX_LEN: {cat_hex_len} (Capacity: {16**cat_hex_len})")
    print(f"  -> Strategy: Fill-Full (Cycle through data to fill all slots)")
    
    for cat_name, cat_data in category_map.items():
        cat_dir = CATEGORIES_DIR / cat_name
        ensure_dir(cat_dir)
        # 使用 write_files_fill_full 模式 (需要修改 write_files 函数支持强制填满)
        write_files(cat_data, cat_dir, cat_hex_len, cat_shard_depth, fill_full=True)

    # 3. 生成规则文件
    print("\nGenerating rules.txt...")
    rule_global = generate_cf_rule(False, global_hex_len, global_shard_depth)
    # 这里的 categories 列表仅用于生成规则中的匹配逻辑，不需要再传递 hex_len，因为规则里已经是固定的了
    rule_category = generate_cf_rule(True, cat_hex_len, cat_shard_depth, categories=list(category_map.keys()))
    
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
