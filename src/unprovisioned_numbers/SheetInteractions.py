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

# +
from datetime import datetime
from os.path import abspath, join

import pandas as pd

class SheetInteractions:
    
    agency_id_map = join(abspath(""), "data", "agency_id_map.json")
    
    def append_new_upn(self, new_row):
        new_row_dict = {
            "from": new_row[0],
            "to": new_row[1],
            "account": new_row[2],
            "log_line": new_row[3],
            "service": self.source_process,
            "date": datetime.now().strftime("%d/%m/%Y %I:%M:%S"),
            "status": 1
        }
        
        self.unprovisioned_number_df.loc[
            len(self.unprovisioned_number_df)] = new_row_dict
    
    @classmethod
    def mapping_agency_ids_to_readable_names(cls, x):
        if cls.agency_id_map.get(x):
            return cls.agency_id_map[x]
        else:
            return x
        
    def apply_agency_mapping_to_upn_df(self):
        self.unprovisioned_number_df["customer_names"] = self.unprovisioned_number_df[
            "account"].apply(self.mapping_agency_ids_to_readable_names)
    
    def make_values_json_readable(self):
        update_values = self.unprovisioned_number_df.fillna("").values.tolist()
        update_values.insert(0, self.unprovisioned_number_df.keys().tolist())
        
        return update_values
    
    def get_known_unprovisioned_numbers(self):
        self.filtered_df = self.unprovisioned_number_df[
            (self.unprovisioned_number_df['service'] == self.source_process)
            & (self.unprovisioned_number_df['status'] == "1")]
        
        return [(str(row['from']), str(row['to'])) for i, row in self.filtered_df.iterrows()]


# -

import os

os.path.abspath('')
