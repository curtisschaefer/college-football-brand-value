# +
import os
import requests
import pandas as pd
from dotenv import load_dotenv
import ast
# Load API key
load_dotenv()
API_KEY = os.getenv("CFB_API_KEY")
headers = {"Authorization": f"Bearer {API_KEY}"}

# Create data directory
os.makedirs("../data", exist_ok=True)

# --- Generic GET helper ---
def get_json_df(url, params):
    res = requests.get(url, headers=headers, params=params)
    return pd.DataFrame(res.json()) if res.status_code == 200 else pd.DataFrame()


# --- API-specific functions ---
def get_sp_ratings(year):
    return get_json_df("https://api.collegefootballdata.com/ratings/sp", {"year": year})

def get_elo_ratings(year):
    return get_json_df("https://api.collegefootballdata.com/ratings/elo", {"year": year})

def get_win_probs(year):
    return get_json_df("https://api.collegefootballdata.com/metrics/wp/pregame", {"year": year})

def get_lines(year):
    return get_json_df("https://api.collegefootballdata.com/lines", {"year": year})

def get_rankings(year):
    return get_json_df("https://api.collegefootballdata.com/rankings", {"year": year})

def get_media_info(year):
    return get_json_df("https://api.collegefootballdata.com/games/media", {"year": year})

def get_games(year):
    return get_json_df("https://api.collegefootballdata.com/games", {"year": year, "seasonType": "both"})

def get_fbs_teams():
    url = "https://api.collegefootballdata.com/teams"
    df = get_json_df(url, params={})
    return df[df["classification"] == "fbs"]["school"].tolist()

# --- Get FBS team names for filtering ---
fbs_teams = get_fbs_teams()

# --- Storage ---
all_data = {
    "games": [],
    "sp": [],
    "elo": [],
    "wp": [],
    "lines": [],
    "rankings": [],
    "media": []
}

# --- Main loop ---
for year in range(2010, 2025):
    print(f"📅 Pulling data for {year}...")

    # Games — filter by classification
    games = get_games(year)
    games = games[
        (games["homeClassification"] == "fbs") |
        (games["awayClassification"] == "fbs")
    ].copy()
    games["season"] = year
    all_data["games"].append(games)

    # SP+
    sp = get_sp_ratings(year)
    sp = sp[sp["team"].isin(fbs_teams)].copy()
    sp["season"] = year
    all_data["sp"].append(sp)

    # Elo
    elo = get_elo_ratings(year)
    elo = elo[elo["team"].isin(fbs_teams)].copy()
    elo["season"] = year
    all_data["elo"].append(elo)

    # Win probabilities
    wp = get_win_probs(year)
    if "team" in wp.columns:
        wp = wp[wp["team"].isin(fbs_teams)].copy()
    wp["season"] = year
    all_data["wp"].append(wp)

    # Lines
    lines = get_lines(year)
    lines = lines[
        lines["homeTeam"].isin(fbs_teams) | lines["awayTeam"].isin(fbs_teams)
    ].copy()
    lines["season"] = year
    all_data["lines"].append(lines)

    # Rankings — keep in original nested format, no filter
    rankings = get_rankings(year)
    rankings["season"] = year
    all_data["rankings"].append(rankings)

    # Media/TV
    media = get_media_info(year)
    media = media[
        media["homeTeam"].isin(fbs_teams) | media["awayTeam"].isin(fbs_teams)
    ].copy()
    media["season"] = year
    all_data["media"].append(media)

# --- Save all datasets ---
for key, df_list in all_data.items():
    if df_list:
        df = pd.concat(df_list, ignore_index=True)
        df.to_csv(f"../data/{key}_2010_2024.csv", index=False)
        print(f"✅ Saved {key} to ../data/{key}_2010_2024.csv")
    else:
        print(f"❌ No data collected for {key}")

# -

lines.columns

# --- Load all CSVs ---
games = pd.read_csv("../data/games_2010_2024.csv")
wp = pd.read_csv("../data/wp_2010_2024.csv")
lines = pd.read_csv("../data/lines_2010_2024.csv")
media = pd.read_csv("../data/media_2010_2024.csv")
rankings_raw = pd.read_csv("../data/rankings_2010_2024.csv")


# +
# rankings_raw is a nested format — each row is a poll, so flatten it


rankings_expanded = []

for _, row in rankings_raw.iterrows():
    # Parse 'polls' column, which is a list of polls
    try:
        polls = ast.literal_eval(row["polls"]) if isinstance(row["polls"], str) else row["polls"]
        for poll in polls:
            ranks = poll.get("ranks", [])
            for team in ranks:
                team["season"] = row["season"]
                team["week"] = row["week"]
                team["seasonType"] = row["seasonType"]
                team["poll"] = poll.get("poll")
                rankings_expanded.append(team)
    except Exception as e:
        print(f"⚠️ Error on row {row.name}: {e}")
        continue

rankings = pd.DataFrame(rankings_expanded)

# -

rankings = rankings[rankings["poll"] == "AP Top 25"].copy()

# +
merged = games.copy()

merged = merged.merge(
    media[["season", "week", "seasonType", "homeTeam", "awayTeam", "mediaType", "outlet"]],
    on=["season", "week", "seasonType", "homeTeam", "awayTeam"],
    how="left"
)
merged = merged.merge(
    lines[["season", "week", "seasonType", "homeTeam", "awayTeam", "lines"]],
    on=["season", "week", "seasonType", "homeTeam", "awayTeam"],
    how="left"
)
merged = merged.merge(
    wp[["season", "week", "seasonType", "homeTeam", "awayTeam", "homeWinProbability"]],
    on=["season", "week", "seasonType", "homeTeam", "awayTeam"],
    how="left"
)
# Home rankings
home_ranks = rankings.rename(columns={"school": "homeTeam", "rank": "homeRank"})
merged = merged.merge(
    home_ranks[["season", "week", "seasonType", "homeTeam", "homeRank"]],
    on=["season", "week", "seasonType", "homeTeam"],
    how="left"
)

# Away rankings
away_ranks = rankings.rename(columns={"school": "awayTeam", "rank": "awayRank"})
merged = merged.merge(
    away_ranks[["season", "week", "seasonType", "awayTeam", "awayRank"]],
    on=["season", "week", "seasonType", "awayTeam"],
    how="left"
)
merged["homeRanked"] = merged["homeRank"].notna()
merged["awayRanked"] = merged["awayRank"].notna()
merged["rankedTeamCount"] = merged[["homeRanked", "awayRanked"]].sum(axis=1)
merged["isRankedMatchup"] = merged["homeRanked"] & merged["awayRanked"]

# -

pd.set_option("display.max_columns", None)
merged.tail()

merged.columns

merged.to_csv("../data/merged_weekly_games_with_rankings.csv", index=False)

wp

fbs_teams


