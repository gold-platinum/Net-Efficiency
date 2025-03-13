"""
@author: Venkat
purpose: Getting training data and manipulating it to add mvp
"""

import pandas as pd
from nba_api.stats.endpoints import playercareerstats, playerawards
from nba_api.stats.static import players
import time


def fetch_player_stats(season):
    player_stats = []

    for player in players.get_players():
        try:
            stats = playercareerstats.PlayerCareerStats(player['id']).get_data_frames()[0]
            stats_season = stats[stats['SEASON_ID'] == f"{season}-{str(int(season[-2:]) + 1).zfill(2)}"]

            if not stats_season.empty:
                stats_season['PLAYER_NAME'] = player['full_name']
                player_stats.append(stats_season)
                print(player['full_name'])

        except Exception as e:
            print(f"{player} was not found")

        time.sleep(1)

    all_player_stats = pd.DataFrame(player_stats)
    return all_player_stats


def fetch_mvp_stats(season):
    mvp_data = playerawards.PlayerAwards(season=season).get_data_frames()[0]
    mvp_players = mvp_data[mvp_data['AWARD_TYPE'] == 1]  # MVP award type
    return mvp_players[['PLAYER_ID', 'PLAYER_NAME', 'TEAM_ID', 'AWARD_NAME', 'SEASON_ID']]


def fetch_query():
    all_data = []
    all_mvp_seasons = []

    for season in range(1990, 2024):
        season_str = str(season)

        player_stats = fetch_player_stats(season_str)
        all_data.append(player_stats)

        mvp_stats = fetch_mvp_stats(season_str)
        mvp_stats['SEASON'] = season_str
        all_mvp_seasons.append(mvp_stats)

        time.sleep(3)

    all_player_stats_df = pd.concat(all_data, ignore_index=True)
    mvp_data_df = pd.concat(all_mvp_seasons, ignore_index=True)

    all_player_stats_df.to_csv("../data/raw/player_stats_1990_2024.csv", index=False)
    mvp_data_df.to_csv("../data/raw/mvp_data_1990_2024.csv", index=False)
    print("✅ Data collection completed!")

if __name__ == "__main__":
    fetch_query()