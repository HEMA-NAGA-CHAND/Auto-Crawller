from prefect import flow, task
import requests
import csv
import re
import pandas as pd
import streamlit as st
from io import BytesIO
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
import matplotlib.pyplot as plt
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import os
import time
import math

RESULT_FILE = "users.csv"
LAST_UPDATE_FILE = "last_update.txt"

HYBRID_INITIAL_PAGES = 5

COOKIE = "_gcl_au=1.1.1127575750.1756710274.1342891669.1759327485.1759327506; cf_clearance=55DsFpEYPyQNUfcQbJwtQA0UV0cruFTkoAn2TNoo7Co-1760079769-1.2.1.1-KpqKppjZuY6258dEQhh8IxtcNCRB_.rL1TX5ad6J_TGSUnbxch0LHAwx3V8Tm99CSuIjxm2LCh1JMwWRG8WAiJB6GGBbhIKPYE9M9B8qtKL4Pv1jeOnY1OYBR..MFS9QT40YLAeDwIT9qzhiOi.sl_trEiRmjGyYW.aeDDL.RCWUyXjc44EfmOrJDJhyky5OPfQXqjq_4AjbD_SdCpH.dNyC4u4Hy9W0ArA0vjH0GrY; _ga_0F9XESWZ11=GS2.2.s1761152256$o2$g0$t1761152256$j60$l0$h0; uid=6262479; _clck=jkpot6%5E2%5Eg0l%5E0%5E2070; g_state={\"i_l\":0,\"i_ll\":1763187217422}; _gid=GA1.2.794990737.1763627211; _gat_UA-141612136-1=1; SESS93b6022d778ee317bf48f7dbffe03173=9d0e709cbb791f1a990a28122d062f4b; Authorization=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJjb2RlY2hlZi5jb20iLCJzdWIiOiI2MjYyNDc5IiwidXNlcm5hbWUiOiJiel9uYWdhY2hhbmQiLCJpYXQiOjE3NjM2MjcyMTYsIm5iZiI6MTc2MzYyNzIxNiwiZXhwIjoxNzY1NjIxNjE2fQ.93_hBasUUHUe1bg_-kRzP-Ki8LR3X0p6rWQV73LNwg4; rzp_unified_session_id=RhvbLWbgtEA8cC; TawkConnectionTime=0; twk_uuid_668d037a7a36f5aaec9634a5=%7B%22uuid%22%3A%221.Swz4oF5I1qo7Pzm1dhEfnZWcrrr4uhrHu5keqqQTfhQRXcZ9yUIit16wj8HNT0QOdTbiX3ZJYHhQbed6YvJ8sgsr520n6l1MXyRn0Kun3ETyuGNqJdgA5%22%2C%22version%22%3A3%2C%22domain%22%3A%22codechef.com%22%2C%22ts%22%3A1763627216067%7D; userkey=56a7ba633e70a409025ae6f4d029a938; _ga_C8RQQ7NY18=GS2.1.s1763627211$o74$g1$t1763627243$j28$l0$h0; _ga=GA1.2.832090242.1756710274"

AUTH_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJjb2RlY2hlZi5jb20iLCJzdWIiOiI2MjYyNDc5IiwidXNlcm5hbWUiOiJiel9uYWdhY2hhbmQiLCJpYXQiOjE3NjM2MjcyMTYsIm5iZiI6MTc2MzYyNzIxNiwiZXhwIjoxNzY1NjIxNjE2fQ.93_hBasUUHUe1bg_-kRzP-Ki8LR3X0p6rWQV73LNwg4"

DEFAULT_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36"),
    "Authorization": AUTH_TOKEN,
    "x-csrf-token": "7a621344a3795105d1b7b28521e987d546b9cfdd19b4a2fa2eb6ef29f9035f5f",
    "x-requested-with": "XMLHttpRequest",
    "Cookie": COOKIE
}

def parse_time_to_seconds(tstr):
    if pd.isna(tstr) or str(tstr).strip() == "":
        return float("inf")
    parts = str(tstr).strip().split(":")
    try:
        parts = list(map(int, parts))
    except:
        return float("inf")
    if len(parts) == 3:
        h, m, s = parts
    elif len(parts) == 2:
        h = 0
        m, s = parts
    else:
        return float("inf")
    return h * 3600 + m * 60 + s

def safe_filename(s):
    return re.sub(r"[^\w\-_\. ]", "_", str(s))

# === Fetches multiple leaderboard pages & saves cleaned user data to CSV ===

@task
def fetch_users_pages(contest_code: str, div: str, pages: int = HYBRID_INITIAL_PAGES, fetch_all: bool = False):
    all_users = []
    page = 1
    headers = DEFAULT_HEADERS.copy()
    while True:
        url = f"https://www.codechef.com/api/rankings/{contest_code}{div}?itemsPerPage=100&order=asc&page={page}&sortBy=rank"
        try:
            resp = requests.get(url, headers=headers, timeout=30)
        except Exception as e:
            print("Request error:", e)
            break
        if resp.status_code != 200:
            print(f"Request failed (status {resp.status_code}) for page {page}")
            break
        data = resp.json()
        users = data.get("list", [])
        if not users:
            break
        all_users.extend(users)
        print(f"Fetched page {page} -> {len(users)} users")
        page += 1
        if not fetch_all and page > pages:
            break
        if fetch_all and page > 2000:
            print("Reached page limit safety cut-off")
            break
    print(f"Total fetched users: {len(all_users)}")
    return all_users


@task
def save_users_to_csv(all_users, filename=RESULT_FILE):
    if not all_users:
        print("No users to save.")
        return None

    cols = ["rank", "user_handle", "name", "score", "total_time",
            "penalty", "country", "institution", "rating"]

    rows = []
    for user in all_users:
        rating_html = user.get("rating", "")
        rating_match = re.search(r'(\d+)&#9733;', str(rating_html))
        rating_clean = rating_match.group(1) if rating_match else ""

        html_handle = user.get("html_handle", "")
        handle_match = re.search(r"<span class='m-username--link'>([^<]+)</span>", str(html_handle))
        handle_clean = handle_match.group(1) if handle_match else user.get("user_handle", "")

        row = {
            "rank": user.get("rank", ""),
            "user_handle": handle_clean,
            "name": user.get("name", ""),
            "score": user.get("score", 0),
            "total_time": user.get("total_time", ""),
            "penalty": user.get("penalty", ""),
            "country": user.get("country", ""),
            "institution": user.get("institution", ""),
            "rating": rating_clean
        }
        rows.append(row)

    df = pd.DataFrame(rows, columns=cols)
    df["institution"] = df["institution"].fillna("").astype(str)
    df.to_csv(filename, index=False, encoding="utf-8")

    with open(LAST_UPDATE_FILE, "w", encoding="utf-8") as f:
        f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    print(f"Saved {len(df)} rows to {filename}")
    return filename

# === Analyzes the CSV and prepares all summary metrics, rankings, and charts data ===

@task
def analyze_csv(filename=RESULT_FILE):
    if not os.path.exists(filename):
        print("No result file found")
        return None

    df = pd.read_csv(filename)
    if df.empty:
        print("Empty dataframe")
        return None

    df["score"] = pd.to_numeric(df["score"], errors="coerce").fillna(0)
    df["total_secs"] = df["total_time"].apply(parse_time_to_seconds)

    top_users = df.sort_values(by=["score", "total_secs"], ascending=[False, True]).head(10)
    country_counts = df["country"].fillna("Unknown").value_counts()

    institution_counts = df["institution"].fillna("").value_counts()
    institution_avg_score = df.groupby("institution")["score"].mean().sort_values(ascending=False)

    fastest_users = df[df["total_secs"] != float("inf")].sort_values(by="total_secs").head(10)

    total_participants = len(df)
    highest_score = int(df["score"].max()) if not df["score"].empty else 0
    avg_score = float(df["score"].mean())
    unique_countries = df["country"].nunique()
    unique_institutions = df["institution"].nunique()

    analysis = {
        "df": df,
        "top_users": top_users,
        "country_counts": country_counts,
        "institution_counts": institution_counts,
        "institution_avg_score": institution_avg_score,
        "fastest_users": fastest_users,
        "summary": {
            "total_participants": total_participants,
            "highest_score": highest_score,
            "avg_score": avg_score,
            "unique_countries": unique_countries,
            "unique_institutions": unique_institutions
        }
    }

    print("Analysis complete")
    return analysis

# === Prefect flow wrapper + background scheduler that auto-fetches every 60 seconds ===

@flow
def auto_crawler_flow(contest_code: str, div: str, pages: int = HYBRID_INITIAL_PAGES, fetch_all: bool = False):
    users = fetch_users_pages(contest_code, div, pages=pages, fetch_all=fetch_all)
    fname = save_users_to_csv(users)
    analysis = analyze_csv(fname)
    return analysis

def scheduled_quick_fetch():
    try:
        auto_crawler_flow("START211", "B", pages=HYBRID_INITIAL_PAGES, fetch_all=False)
    except Exception as e:
        print("Scheduled fetch error:", e)

scheduler = BackgroundScheduler()
scheduler.add_job(scheduled_quick_fetch, "interval", seconds=60)
scheduler.start()


# === Streamlit dashboard UI + charts + filters + PDF generator ===

st.set_page_config(layout="wide", page_title="CodeChef Analyzer")
st.title("🏆 CodeChef Contest Analyzer Dashboard")

contest_code = st.text_input("Contest Code", value="START211")
div = st.text_input("Division", value="B")
pages_to_fetch = st.number_input("Initial pages to fetch (hybrid)", min_value=1, max_value=200, value=HYBRID_INITIAL_PAGES)
fetch_full_button = st.button("Fetch Full Dataset (may take long)")

if st.button("Fetch & Analyze (quick)"):
    with st.spinner("Fetching (quick)..."):
        auto_crawler_flow(contest_code, div, pages=pages_to_fetch, fetch_all=False)
    st.success("Quick fetch finished — dashboard will refresh automatically")

if fetch_full_button:
    with st.spinner("Fetching ALL pages (this can take long)..."):
        auto_crawler_flow(contest_code, div, pages=pages_to_fetch, fetch_all=True)
    st.success("Full fetch finished — dashboard will refresh automatically")

refresh_interval_seconds = 600
if "last_refresh" not in st.session_state:
    st.session_state["last_refresh"] = time.time()

if time.time() - st.session_state["last_refresh"] > refresh_interval_seconds:
    st.session_state["last_refresh"] = time.time()
    st.rerun()

analysis = None
if os.path.exists(RESULT_FILE):
    analysis = analyze_csv(RESULT_FILE)

last_updated = None
if os.path.exists(LAST_UPDATE_FILE):
    with open(LAST_UPDATE_FILE, "r", encoding="utf-8") as f:
        last_updated = f.read().strip()


if analysis:
    df = analysis["df"]
    summary = analysis["summary"]

    st.markdown("### Summary Metrics")
    c1, c2, c3 = st.columns(3)
    c1.metric("Total participants", summary["total_participants"])
    c2.metric("Highest score", summary["highest_score"])
    c3.metric("Average score", f"{summary['avg_score']:.2f}")

    c4, c5, c6 = st.columns(3)
    c4.metric("Countries represented", summary["unique_countries"])
    c5.metric("Institutions represented", summary["unique_institutions"])
    c6.metric("Last updated", last_updated if last_updated else "N/A")

    st.markdown("---")

    left_col, right_col = st.columns((2, 1))

    institutions = sorted([inst for inst in analysis["institution_counts"].index.tolist() if str(inst).strip() != ""])
    institutions_display = ["(All)"] + institutions
    selected_institution = right_col.selectbox("Filter by Institution", institutions_display)

    if selected_institution and selected_institution != "(All)":
        df_filtered = df[df["institution"] == selected_institution].copy()
    else:
        df_filtered = df.copy()

    left_col.subheader("📋 Rankings Table (filtered)")
    left_col.dataframe(df_filtered.reset_index(drop=True).head(500))

    right_col.subheader("🏫 Institution Leaderboard")
    if selected_institution and selected_institution != "(All)":
        inst_top = df[df["institution"] == selected_institution].sort_values(by=["score", "total_secs"], ascending=[False, True]).head(20)
        right_col.write(f"Top participants for **{selected_institution}**")
        right_col.dataframe(inst_top[["rank", "user_handle", "score", "total_time"]].reset_index(drop=True))
    else:
        inst_counts = analysis["institution_counts"].head(20)
        right_col.write("Top institutions (by participants)")
        right_col.dataframe(inst_counts.rename_axis("institution").reset_index(name="participants"))

    st.markdown("### Visuals")

    row1_col1, row1_col2 = st.columns((1, 1))

    with row1_col1:
        st.subheader("Scatter: Score vs Total Time")
        fig_sc, ax_sc = plt.subplots(figsize=(5, 3))
        plot_df = df[df["total_secs"] != float("inf")].copy()
        ax_sc.scatter(plot_df["total_secs"], plot_df["score"], alpha=0.6, s=18)
        ax_sc.set_xlabel("Total time (s)", fontsize=9)
        ax_sc.set_ylabel("Score", fontsize=9)
        ax_sc.set_title("Score vs Total Time", fontsize=10)
        try:
            ax_sc.set_xscale("log")
        except Exception:
            pass
        ax_sc.tick_params(axis='x', labelsize=8)
        ax_sc.tick_params(axis='y', labelsize=8)
        fig_sc.tight_layout()
        st.pyplot(fig_sc)

    with row1_col2:
        st.subheader("Score distribution")
        fig_h, ax_h = plt.subplots(figsize=(5, 3))
        ax_h.hist(df["score"].fillna(0), bins=18)
        ax_h.set_xlabel("Score", fontsize=9)
        ax_h.set_ylabel("Count", fontsize=9)
        ax_h.tick_params(axis='x', labelsize=8)
        ax_h.tick_params(axis='y', labelsize=8)
        fig_h.tight_layout()
        st.pyplot(fig_h)

    st.markdown("---")

    row2_col1, row2_col2 = st.columns((1, 1))

    with row2_col1:
        st.subheader("Countries (top 15)")
        top_countries = analysis["country_counts"].head(15)
        fig_c, ax_c = plt.subplots(figsize=(5, 3))
        ax_c.bar(top_countries.index.astype(str), top_countries.values)
        ax_c.set_xticklabels(top_countries.index.astype(str), rotation=45, ha="right", fontsize=8)
        ax_c.set_ylabel("Participants", fontsize=9)
        ax_c.tick_params(axis='y', labelsize=8)
        fig_c.tight_layout()
        st.pyplot(fig_c)

    with row2_col2:
        st.subheader("Top Colleges")
        mode = st.selectbox("Show top colleges by:", ["Participants", "Average Score"])
        if mode == "Participants":
            top_inst = analysis["institution_counts"].head(15)
            fig_i, ax_i = plt.subplots(figsize=(5, 3))
            ax_i.bar(top_inst.index.astype(str), top_inst.values)
            ax_i.set_xticklabels(top_inst.index.astype(str), rotation=45, ha="right", fontsize=8)
            ax_i.set_ylabel("Participants", fontsize=9)
            ax_i.tick_params(axis='y', labelsize=8)
            fig_i.tight_layout()
            st.pyplot(fig_i)
        elif mode == "Average Score":
            inst_avg = analysis["institution_avg_score"].drop(labels=[""], errors="ignore").head(15)
            fig_i2, ax_i2 = plt.subplots(figsize=(5, 3))
            ax_i2.bar(inst_avg.index.astype(str), inst_avg.values)
            ax_i2.set_xticklabels(inst_avg.index.astype(str), rotation=45, ha="right", fontsize=8)
            ax_i2.set_ylabel("Avg Score", fontsize=9)
            ax_i2.tick_params(axis='y', labelsize=8)
            fig_i2.tight_layout()
            st.pyplot(fig_i2)

    st.markdown("---")
    st.subheader("📄 Generate Custom PDF Report")
    include_top = st.checkbox("Include Top 10 Users", True)
    include_country = st.checkbox("Include Country Participation (chart)", True)
    include_institution = st.checkbox("Include Institution Leaderboard (selected)", True)
    include_scatter = st.checkbox("Include Scatter Plot (Score vs Time)", True)

    if st.button("Download PDF Report"):
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4)
        styles = getSampleStyleSheet()
        elements = []

        elements.append(Paragraph(f"CodeChef Contest Report: {contest_code}{div}", styles["Title"]))
        elements.append(Spacer(1, 12))
        elements.append(Paragraph("Summary Metrics", styles["Heading2"]))

        summ = analysis["summary"]
        elements.append(Paragraph(f"Total participants: {summ['total_participants']}", styles["Normal"]))
        elements.append(Paragraph(f"Highest score: {summ['highest_score']}", styles["Normal"]))
        elements.append(Paragraph(f"Average score: {summ['avg_score']:.2f}", styles["Normal"]))
        elements.append(Paragraph(f"Countries represented: {summ['unique_countries']}", styles["Normal"]))
        elements.append(Paragraph(f"Institutions represented: {summ['unique_institutions']}", styles["Normal"]))
        elements.append(Spacer(1, 12))

        if include_top:
            elements.append(Paragraph("Top 10 Users", styles["Heading2"]))
            top_table = [analysis["top_users"].columns.tolist()] + analysis["top_users"].values.tolist()
            elements.append(Table(top_table, style=[
                ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey)
            ]))
            elements.append(Spacer(1, 12))

        if include_country:
            pie_buf = BytesIO()
            fig_p, ax_p = plt.subplots(figsize=(6, 6))
            analysis["country_counts"].plot.pie(autopct="%1.1f%%", ax=ax_p, ylabel="")
            fig_p.savefig(pie_buf, format="PNG", bbox_inches="tight")
            plt.close(fig_p)
            pie_buf.seek(0)
            elements.append(Paragraph("Country Participation", styles["Heading2"]))
            elements.append(Image(pie_buf, width=400, height=400))
            elements.append(Spacer(1, 12))

        if include_institution and selected_institution and selected_institution != "(All)":
            elements.append(Paragraph(f"Institution Leaderboard: {selected_institution}", styles["Heading2"]))
            inst_df = df[df["institution"] == selected_institution].sort_values(by=["score", "total_secs"], ascending=[False, True]).head(50)
            if not inst_df.empty:
                table_data = [inst_df[["rank", "user_handle", "score", "total_time"]].columns.tolist()] + inst_df[["rank", "user_handle", "score", "total_time"]].values.tolist()
                elements.append(Table(table_data, style=[
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
                    ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey)
                ]))
                elements.append(Spacer(1, 12))

        if include_scatter:
            sc_buf = BytesIO()
            fig_sc.savefig(sc_buf, format="PNG", bbox_inches="tight")
            plt.close(fig_sc)
            sc_buf.seek(0)
            elements.append(Paragraph("Score vs Total Time", styles["Heading2"]))
            elements.append(Image(sc_buf, width=450, height=250))
            elements.append(Spacer(1, 12))

        doc.build(elements)
        buffer.seek(0)
        st.download_button("📥 Download PDF", data=buffer, file_name=f"{safe_filename(contest_code+div)}_report.pdf", mime="application/pdf")

else:
    st.info("Waiting for automatic data fetch (first quick scheduled run within 60s).")
