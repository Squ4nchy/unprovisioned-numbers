# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.0
#   kernelspec:
#     display_name: upn-env
#     language: python
#     name: upn-env
# ---

# + tags=[]
import warnings
from datetime import datetime

import pandas as pd

from unprovisioned_numbers.UnprovisionedNumbers import UnprovisionedNumbers
from unprovisioned_numbers.SlackWebHook import SlackWebHook

warnings.simplefilter(action='ignore', category=FutureWarning)

if __name__ == "__main__":
    for process in ["OracleIsrFaceSync", "OpenCloudSmsSync"]:
        
        with UnprovisionedNumbers(process) as upn:
            confirmed_provisioned = upn.query_loki_confirm_provisioned_numbers()
            unknown_nums = upn.compare_unique_with_known()
            
            if len(unknown_nums) or not confirmed_provisioned.empty:
                unprovisioned_number_url = (
                    "<google sheet location>")
                opening_text = f"""*{process}*\n{unprovisioned_number_url}"""
                SlackWebHook.post_message_to_channel(opening_text)
                
            
            if len(unknown_nums):                
                new_upn_df = pd.DataFrame(unknown_nums, columns=['from', 'to', 'account', 'log_line'])

                new_upn_update_text = (f"There are new {upn.source_process} "
                                       "unprovisioned numbers added to the Sheet."
                                       "\nSend the new additions to christopher.drummondheeks@vodafone.com.")
                
                SlackWebHook.post_message_to_channel(new_upn_update_text)

                for row in unknown_nums:
                    upn.append_new_upn(row)
                
                upn.apply_agency_mapping_to_upn_df()
                
                upn.update_full_sheet_with_additional_values(upn.oauth_credentials)
                
            if not confirmed_provisioned.empty:
                prov_update_text = ("There are newly provisioned numbers, please "
                                    "filter the date column to today's date "
                                    f"and mark the status as '0' in {unprovisioned_number_url}.")
                
                for i in range(len(upn.unprovisioned_number_df)):
                    if upn.unprovisioned_number_df.iloc[i].values.tolist() in confirmed_provisioned.values.tolist():
                        upn.unprovisioned_number_df.iloc[i]["date_provisioned"] = datetime.now().strftime("%d/%m/%Y %I:%M:%S")
                
                upn.update_full_sheet_with_additional_values(upn.oauth_credentials)
                
                SlackWebHook.post_message_to_channel(prov_update_text)
