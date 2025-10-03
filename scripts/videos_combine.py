
import pandas as pd


combined_df = pd.read_csv('youtube_videos_metadata.csv')

for i in range(9):
    new_df = pd.read_csv(f'youtube_videos_metadata_batch{i}.csv')
    combined_df = pd.concat([combined_df, new_df], ignore_index=True)
    print(len(combined_df))
print(len(combined_df))
combined_df = combined_df.drop(['description', 'dimension', 'embeddable', 'publicStatsViewable', 'recordingDate', 'recordingLocation'], axis=1)

combined_df.to_csv('videos_metadata.csv', index=False)

