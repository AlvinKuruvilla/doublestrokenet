import os
import json
import pandas as pd

RAW_KEY = "./raw_data/csv_raw_and_processed/Data_Raw/keystrokes.csv"
RAW_KEY_HEAD = "./raw_data/csv_raw_and_processed/Data_Raw/keystrokes_header.csv"
RAW_TEST = "./raw_data/csv_raw_and_processed/Data_Raw/test_sections.csv"
RAW_TEST_HEAD = "./raw_data/csv_raw_and_processed/Data_Raw/test_sections_header.csv"

PRC_PATH = "./prc_data/mobile_users/"
PRC_BENCH = "./prc_data/mobile_bench/"
PRC_BENCH_REST = "./prc_data/mobile_bench_rest/"
prc_files = os.listdir(PRC_PATH)
for file in prc_files:
    os.remove(PRC_PATH + file)

# empty clean folder
prc_files = os.listdir(PRC_BENCH)
for file in prc_files:
    os.remove(PRC_BENCH + file)
# assemble the keystrokes dataframe
keystrokes_df = pd.read_csv(
    RAW_KEY, header=None, escapechar="\\", encoding="ISO-8859-1"
)
key_header = pd.read_csv(RAW_KEY_HEAD, header=None)

keystrokes_df.columns = key_header.iloc[1:, 0].values
keystrokes_df = keystrokes_df[
    ["TEST_SECTION_ID", "PRESS_TIME", "RELEASE_TIME", "KEYCODE"]
]
seq_df = (
    keystrokes_df.groupby("TEST_SECTION_ID")
    .apply(lambda x: list(zip(x["PRESS_TIME"], x["RELEASE_TIME"], x["KEYCODE"])))
    .reset_index()
)
seq_df = seq_df.rename(
    columns={0: "SEQUENCE"}
)  # transform the sequence into (timestamp, event_type, keycode)


def event_func(seqe):
    new_seq = []
    # for s in seqe:
    #   new_seq.append((s[0], 1, s[2]))
    #   new_seq.append((s[1], 0, s[2]))
    new_seq = sorted(seqe, key=lambda x: x[0])
    return new_seq


seq_df["SEQUENCE"] = seq_df["SEQUENCE"].apply(event_func)
# assemble the test_section df
mobile_test_section_df = pd.read_csv(
    RAW_TEST, escapechar="\\", quotechar='"', encoding="ISO-8859-1", header=None
)
header_df = pd.read_csv(RAW_TEST_HEAD)
# put header on test sections df
mobile_test_section_df.columns = header_df.iloc[:, 0].values
# join by test_section_id
joined_df = seq_df.merge(
    mobile_test_section_df[["TEST_SECTION_ID", "PARTICIPANT_ID"]],
    on="TEST_SECTION_ID",
    how="left",
)
joined_part_df = joined_df[["PARTICIPANT_ID", "TEST_SECTION_ID", "SEQUENCE"]].groupby(
    ["PARTICIPANT_ID"]
)
for name, group in joined_part_df:
    if len(group) >= 15:
        valid = True
        for i, row in group.iterrows():
            if len(row["SEQUENCE"]) < 50:
                valid = False
                break
        if valid:
            group.to_csv(f"{PRC_PATH}/{name}.csv", index=False)
test_users = json.load(open("data_preproc/typeformer_bench.json"))
# transform keys into ints
test_users = {int(k): list(map(int, v)) for k, v in test_users.items()}
for user, test_id_list in test_users.items():
    curr_group = joined_part_df.get_group(user)
    curr_group = curr_group[curr_group["TEST_SECTION_ID"].isin(test_id_list)]
    curr_group.to_csv(f"{PRC_BENCH}/{user}.csv", index=False)
# all mobile users
all_mobile = os.listdir(PRC_PATH)
bench_mobile = os.listdir(PRC_BENCH)
bench_rest_mobile = list(set(all_mobile) - set(bench_mobile))

# copy the files into PRC_BENCH_REST
for file in bench_rest_mobile:
    os.system(f"cp {PRC_PATH}/{file} {PRC_BENCH_REST}/{file}")
