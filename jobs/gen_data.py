import random, csv, datetime, os

random.seed(42)
OUT = "/home/data/bigdata/jobs/raw"
os.makedirs(OUT, exist_ok=True)

# ---- 维度表：地区 ----
regions = [(1, "华东"), (2, "华北"), (3, "华南"), (4, "华中"), (5, "西南"), (6, "东北")]
with open(f"{OUT}/region.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["region_id", "region_name"])
    w.writerows(regions)

# ---- 维度表：用户 2 万 ----
with open(f"{OUT}/users.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["user_id", "user_name", "gender", "age", "region_id", "reg_date"])
    for i in range(1, 20001):
        d = datetime.date(2025, 1, 1) + datetime.timedelta(days=random.randint(0, 365))
        w.writerow([i, f"user_{i}", random.choice(["M", "F"]),
                    random.randint(18, 60), random.randint(1, 6), d.isoformat()])

# ---- 维度表：商品 200 ----
cats = ["手机", "电脑", "家电", "服饰", "食品", "图书"]
prices = {}
with open(f"{OUT}/products.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["product_id", "product_name", "category", "price"])
    for i in range(1, 201):
        p = round(random.uniform(20, 8000), 2)
        prices[i] = p
        w.writerow([i, f"product_{i}", random.choice(cats), p])

# ---- 事实表：订单 10 万 + 明细 30 万 ----
with open(f"{OUT}/orders.csv", "w", newline="", encoding="utf-8") as f, \
     open(f"{OUT}/order_items.csv", "w", newline="", encoding="utf-8") as g:
    wo = csv.writer(f)
    wi = csv.writer(g)
    wo.writerow(["order_id", "user_id", "order_date", "order_status"])
    wi.writerow(["item_id", "order_id", "product_id", "quantity", "unit_price"])
    item_id = 1
    for oid in range(1, 100001):
        d = datetime.date(2026, 1, 1) + datetime.timedelta(days=random.randint(0, 89))
        wo.writerow([oid, random.randint(1, 20000), d.isoformat(),
                     random.choice(["paid", "paid", "paid", "refund", "cancel"])])
        for _ in range(random.randint(1, 3)):
            pid = random.randint(1, 200)
            wi.writerow([item_id, oid, pid, random.randint(1, 3),
                         round(prices[pid] * random.uniform(0.9, 1.1), 2)])
            item_id += 1

print("generated", item_id - 1, "items")
