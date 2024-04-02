import os
import csv
import pandas as pd

from tqdm import tqdm

PRC_PATH = "./prc_data/desk_users/"
RAW_PATH = "./raw_data/Keystrokes/files/"
# all the user sessios count:
print("All users session count:", len(os.listdir(RAW_PATH)))
# empty clean folder
prc_files = os.listdir(PRC_PATH)
for file in prc_files:
    os.remove(PRC_PATH + file)


def keylogs_to_sesh(keylog_df: pd.DataFrame) -> None:
    gr_df = keylog_df.groupby(["PARTICIPANT_ID", "TEST_SECTION_ID"])

    session_data = []
    for part_test, group in gr_df:
        sequence = []
        for idx, row in group.iterrows():
            # press_time = row['PRESS_TIME']
            # release_time = row['RELEASE_TIME']
            # keycode = row['KEYCODE']
            # sequence.append((press_time, press_code,  keycode))
            # sequence.append((release_time, release_code, keycode))
            sequence.append((row["PRESS_TIME"], row["RELEASE_TIME"], row["KEYCODE"]))

        # sort by time
        sequence = sorted(sequence, key=lambda x: x[0])

        # session = participant_id, test_id, sequence
        session_data.append((part_test[0], part_test[1], sequence))

    session_df = pd.DataFrame(
        session_data, columns=["PARTICIPANT_ID", "TEST_SECTION_ID", "SEQUENCE"]
    )

    return session_df


def validate_user_set(session_df: pd.DataFrame) -> None:
    # validate typing sessions for each user
    # filter by the minimum of events per session and the maximum time difference between eventsv - not anymore
    # max_time_diff = 5

    min_events = 20  # minimum number of key events

    valid = True

    # iterate over each row
    for i, row in session_df.iterrows():
        sequence = row["SEQUENCE"]

        events_cnt = len(sequence)
        if events_cnt < min_events:
            valid = False
            break

    return valid


raw_sessions = os.listdir(RAW_PATH)

for session_file in tqdm(raw_sessions):
    with open(RAW_PATH + "/" + session_file, "r", encoding="windows-1252") as f:
        content = f.read()
        # replace \n
        content = content.replace("\t\n\t", "\tnewline\t")
        content = content.replace("\t\t\t", "\ttab\t")

    with open(RAW_PATH + "/" + session_file, "w", encoding="windows-1252") as f:
        f.write(content)
        f.close()

    try:
        df = pd.read_csv(
            RAW_PATH + "/" + session_file,
            sep="\t",
            encoding="windows-1252",
            quoting=csv.QUOTE_NONE,
        )
        session_df = keylogs_to_sesh(df)

        if validate_user_set(session_df):
            session_df.to_csv(PRC_PATH + "/" + session_file)
    except:
        print("Error processing file: " + session_file)
