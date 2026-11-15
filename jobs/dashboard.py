# -*- coding: utf-8 -*-
"""数据看板：从 MySQL 读取 ADS 结果，用 matplotlib 画成六宫格"""
import os
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import font_manager
import pandas as pd
from sqlalchemy import create_engine

FONT_PATH = "/home/data/bigdata/fonts_msyh.ttc"
font_manager.fontManager.addfont(FONT_PATH)
CJK = font_manager.FontProperties(fname=FONT_PATH).get_name()
plt.rcParams["font.family"] = CJK
plt.rcParams["axes.unicode_minus"] = False

TEAL = "#3C8C8C"
DARK = "#1F4E4E"
ACCENT = "#86B6B6"
LIGHT = "#C2DADA"
GREY = "#8A9A9A"
BG = "#FFFFFF"

engine = create_engine("mysql+pymysql://root:123456@127.0.0.1:3306/dw_ads?charset=utf8mb4",
                       future=True)
NUM_COLS = {"gmv", "order_cnt", "uv", "qty", "revenue", "ratio_pct", "users",
            "monetary", "frequency", "recency"}


def q(sql):
    df = pd.read_sql(sql, engine)
    for c in list(df.columns):
        if c in NUM_COLS:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


daily = q("select * from daily_gmv order by order_date")
cat = q("select * from category_rank order by gmv desc")
age = q("select * from age_dist order by age_group")
month = q("select * from month_revenue order by `year_month`")
country_all = q("select * from country_revenue order by revenue desc")
rfm = q("select * from rfm_segment order by revenue desc")

fig = plt.figure(figsize=(16, 9.6), dpi=150, facecolor=BG)
gs = fig.add_gridspec(2, 3, hspace=0.46, wspace=0.30,
                      left=0.055, right=0.973, top=0.825, bottom=0.085)

fig.text(0.055, 0.975, "电商与零售数仓 · 数据看板", ha="left", va="top",
         fontsize=22, fontweight="bold", color=DARK)
fig.text(0.055, 0.925, "数据链路：Spark 离线数仓 ADS 层 →  MySQL（dw_ads）→  图表　｜　"
                       "左侧三图：订单数仓　右侧三图：零售交易数仓",
         ha="left", va="top", fontsize=9.6, color=GREY)
fig.lines.append(plt.Line2D([0.055, 0.973], [0.898, 0.898],
                            transform=fig.transFigure, color=TEAL, lw=1.6))


def style(ax, title):
    ax.set_facecolor(BG)
    for s in ("top", "right"):
        ax.spines[s].set_visible(False)
    for s in ("left", "bottom"):
        ax.spines[s].set_color("#DDDDDD")
        ax.spines[s].set_linewidth(0.8)
    ax.tick_params(colors="#666666", labelsize=8, length=0)
    ax.grid(axis="y", color="#EFEFEF", lw=0.9)
    ax.set_axisbelow(True)
    ax.set_title(title, fontsize=12, fontweight="bold", color=DARK,
                 loc="left", pad=10)


# ---------- 1. 日 GMV ----------
ax = fig.add_subplot(gs[0, 0])
x = pd.to_datetime(daily["order_date"])
y = daily["gmv"] / 1e4
ax.plot(x, y, color=TEAL, lw=1.7)
ax.fill_between(x, y, color=TEAL, alpha=0.12)
ax.set_ylim(y.min() * 0.92, y.max() * 1.04)
style(ax, "日 GMV 趋势　·　万元 / 90 天")
ax.tick_params(axis="x", labelrotation=28, labelsize=7)

# ---------- 2. 品类排行 ----------
ax = fig.add_subplot(gs[0, 1])
order = cat.iloc[::-1]
vals = order["gmv"] / 1e8
bars = ax.barh(order["category"], vals, height=0.6,
               color=[TEAL if i == len(order) - 1 else ACCENT for i in range(len(order))])
for b, r in zip(bars, order["ratio_pct"]):
    ax.text(b.get_width() + max(vals) * 0.02, b.get_y() + b.get_height() / 2,
            f"{r:.1f}%", va="center", fontsize=8.6, color="#555555")
ax.set_xlim(0, max(vals) * 1.2)
ax.grid(axis="y", visible=False)
style(ax, "品类 GMV 排行　·　亿元")

# ---------- 3. 年龄段 ----------
ax = fig.add_subplot(gs[0, 2])
vals = age["gmv"] / 1e8
bars = ax.bar(age["age_group"], vals, color=TEAL, width=0.52)
for b, v in zip(bars, age["order_cnt"]):
    ax.text(b.get_x() + b.get_width() / 2, b.get_height() + vals.max() * 0.02,
            f"{int(v):,}", ha="center", fontsize=8.2, color="#666666")
ax.set_ylim(0, vals.max() * 1.16)
style(ax, "年龄段 GMV 分布　·　亿元（柱顶订单数）")

# ---------- 4. 月度营收 ----------
ax = fig.add_subplot(gs[1, 0])
mx = month["year_month"]
my = month["revenue"] / 1e4
ax.bar(mx, my, color=ACCENT, width=0.62)
ax.plot(mx, my, color=TEAL, lw=1.5, marker="o", ms=3)
peak = int(np.argmax(my.values))
ax.annotate("年末旺季", xy=(peak, my.iloc[peak]), xytext=(10, -26),
            textcoords="offset points", fontsize=8.4, color="#B5651D",
            arrowprops=dict(arrowstyle="->", color="#B5651D", lw=1))
ax.set_ylim(0, my.max() * 1.15)
style(ax, "月度营收　·　万元 / 18 个月")
ax.tick_params(axis="x", labelrotation=45, labelsize=6.8)

# ---------- 5. 国家 TOP10 ----------
ax = fig.add_subplot(gs[1, 1])
top = country_all.head(10).iloc[::-1]
vals = top["revenue"] / 1e6
bars = ax.barh(top["country"], vals, height=0.58, color=ACCENT)
bars[-1].set_color(TEAL)
for b, v, r in zip(bars, vals, top["ratio_pct"]):
    if float(r) >= 3:
        ax.text(b.get_width() + max(vals) * 0.02, b.get_y() + b.get_height() / 2,
                f"{float(r):.0f}%", va="center", fontsize=8.4, color="#555555")
ax.set_xlim(0, max(vals) * 1.16)
ax.grid(axis="y", visible=False)
style(ax, "国家 / 地区营收 TOP10　·　百万元")

# ---------- 6. RFM 分群 ----------
ax = fig.add_subplot(gs[1, 2])
colors = [TEAL, ACCENT, LIGHT, "#DCE9E9"]
vals = rfm["revenue"]
wedges, _ = ax.pie(vals, startangle=90, counterclock=False,
                   colors=colors[:len(rfm)],
                   wedgeprops=dict(width=0.40, edgecolor="white", linewidth=1.4))
total_rev = vals.sum()
for w, seg, rv in zip(wedges, rfm["segment"], vals):
    pct = rv / total_rev * 100
    if pct < 15:
        continue
    ang = (w.theta2 + w.theta1) / 2
    ax.text(0.80 * np.cos(np.deg2rad(ang)), 0.80 * np.sin(np.deg2rad(ang)),
            f"{seg}\n{pct:.0f}%", ha="center", va="center",
            fontsize=8.4, color="white", fontweight="bold")
ax.text(0, 0, f"客户数\n{int(rfm['users'].sum()):,}", ha="center", va="center",
        fontsize=10.5, color=DARK, fontweight="bold")
small = rfm[vals / total_rev * 100 < 15]
legend_txt = "　".join(f"{s} {float(r) / float(total_rev) * 100:.0f}%" for s, r in
                       zip(small["segment"], small["revenue"]))
if legend_txt:
    ax.text(0, -1.26, legend_txt, ha="center", fontsize=8, color="#777777")
ax.set_title("RFM 客户分群　·　按营收占比", fontsize=12, fontweight="bold",
             color=DARK, loc="left", pad=10)

fig.text(0.055, 0.022, "说明：订单数仓为 90 天模拟数据（10 万订单 / 12 万明细）；"
                       "零售数仓为 18 个月真实分布数据（54.8 万行，含取消单与空客户号）",
         fontsize=8.4, color=GREY)

out = "/home/data/bigdata/jobs/dashboard.png"
plt.savefig(out, facecolor=BG)
print("saved:", out, os.path.getsize(out) // 1024, "KB")
