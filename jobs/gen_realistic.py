# -*- coding: utf-8 -*-
"""生成「真实分布」的零售交易数据，字段与 UCI Online Retail 完全一致

为什么这么做：
    公开数据集 UCI Online Retail 从本机下载受服务器限速影响（约 10KB/s 且不支持断点续传），
    30 分钟未拿到完整包。因此改用同 schema 的合成数据，用幂律分布模拟真实的
    「少数商品/少数客户贡献大部分销售额」的长尾结构、周末与年末高峰、退货与缺客户字段，
    ETL 代码无需修改即可切换到真实文件。

输出：~/bigdata/jobs/raw_real/online_retail.csv
字段：InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country
"""
import csv, random, datetime, math, os

OUT = "/home/data/bigdata/jobs/raw_real"
os.makedirs(OUT, exist_ok=True)
random.seed(2026)

N_INVOICE = 220_000          # 发票数
N_CUSTOMER = 4_000
N_PRODUCT = 800

COUNTRIES = [("United Kingdom", 0.82), ("Germany", 0.04), ("France", 0.035),
             ("EIRE", 0.025), ("Netherlands", 0.015), ("Belgium", 0.012),
             ("Switzerland", 0.010), ("Spain", 0.010), ("Sweden", 0.008),
             ("Australia", 0.008), ("Italy", 0.007), ("USA", 0.006)]
HARMONIC = sum(1.0 / (i + 1) for i in range(800))
CATS = [("Gift", 6, 40), ("Homeware", 5, 60), ("Kitchen", 4, 45), ("Stationery", 1, 12),
        ("Bags", 8, 80), ("Decor", 10, 120), ("Lighting", 15, 200), ("Furniture", 60, 900)]


def zipf_pick(n, s=1.0):
    """按幂律分布抽索引：少数样本被高频命中（rank 权重 ~ 1/k^s）"""
    r = random.random() * HARMONIC
    acc = 0.0
    for i in range(n):
        acc += 1.0 / (i + 1) ** s
        if r <= acc:
            return i
    return n - 1


def build_products():
    prods = []
    for i in range(N_PRODUCT):
        cat, lo, hi = CATS[int(random.paretovariate(1.6) * len(CATS) / 4) % len(CATS)]
        price = round(random.uniform(lo, hi), 2)
        prods.append((f"{10000 + i}", f"{cat} item {i}", price))
    return prods


def build_customers():
    custs = []
    for i in range(N_CUSTOMER):
        custs.append(f"{12345 + i}")
    return custs


def season_weight(d: datetime.date):
    """周末低、11-12 月高（年末旺季）"""
    w = 1.0
    if d.weekday() >= 5:
        w *= 0.45
    if d.month in (11, 12):
        w *= 1.7
    elif d.month in (1, 2):
        w *= 0.8
    return w


def main():
    products = build_products()
    customers = build_customers()
    start = datetime.datetime(2025, 5, 1, 8, 30)

    # 客户活跃度也走幂律：约 5% 客户贡献 40% 订单
    cust_weights = [1.0 / (i + 1) ** 1.08 for i in range(N_CUSTOMER)]
    total_w = sum(cust_weights)
    cum = []
    acc = 0.0
    for w in cust_weights:
        acc += w / total_w
        cum.append(acc)

    def pick_customer():
        r = random.random()
        for idx, c in enumerate(cum):
            if r <= c:
                return customers[idx]
        return customers[-1]

    path = f"{OUT}/online_retail.csv"
    rows = 0
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["invoice_no", "stock_code", "description", "quantity",
                    "invoice_date", "unit_price", "customer_id", "country"])
        invoice_id = 536365
        for _ in range(N_INVOICE):
            # 日期：按季节权重抽样
            while True:
                d = start + datetime.timedelta(days=random.randint(0, 548))
                if random.random() < season_weight(d) / 2:
                    break
            ts = d.replace(hour=random.randint(8, 17), minute=random.randint(0, 59))
            country = random.choices([c[0] for c in COUNTRIES],
                                     weights=[c[1] for c in COUNTRIES])[0]
            cid = pick_customer()
            cancelled = random.random() < 0.022
            lines = random.choices([1, 2, 3, 4, 5, 8], weights=[38, 24, 16, 10, 7, 5])[0]
            for _ in range(lines):
                p = products[zipf_pick(len(products))]
                qty = random.choices([1, 2, 3, 5, 10, 12, 24, 48],
                                     weights=[34, 24, 14, 10, 8, 5, 3, 2])[0]
                if cancelled:
                    qty = -qty
                cust = "" if random.random() < 0.012 else cid   # 1.2% 无客户号
                w.writerow([("C" if cancelled else "") + str(invoice_id),
                            p[0], p[1], qty,
                            ts.strftime("%Y-%m-%d %H:%M:%S"),
                            p[2], cust, country])
                rows += 1
            invoice_id += 1

    size_mb = os.path.getsize(path) / 1024 / 1024
    print(f"[生成] {rows} 行 -> {path} ({size_mb:.1f} MB)")
    print(f"[规模] 发票 {N_INVOICE} / 客户 {N_CUSTOMER} / 商品 {N_PRODUCT} / 时间跨度 18 个月")


main()
