
import pandas as pd

channels_df = pd.read_csv("estonian_youtubers.csv")

channels_df = channels_df[channels_df['views'] > 500]

channels_df = channels_df[~channels_df['title'].str.contains("topic", na=False)]

channels_df = channels_df.query('videos <= 1000')








