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
import re

from unprovisioned_numbers.LokiApiInteraction import LokiApiInteraction
from unprovisioned_numbers.GoogleSheetsApi import GoogleSheetsApi
from unprovisioned_numbers.SheetInteractions import SheetInteractions

class UnprovisionedNumbers(LokiApiInteraction, GoogleSheetsApi, SheetInteractions):
    """
    Unique set of un-provisioned numbers, based on a Grafana log line, which can be compared against
    a list of known un-provisioned numbers.
    """
    
    def __init__(self, source_process):
        super().__init__(source_process)
        self.log_line_string = self.query_loki_for_error_log_line()
        self.oauth_credentials = self.oauth_for_scope_spreadsheet()
        self.add_current_values_to_obj()
        self.known_unprovisioned_numbers = self.get_known_unprovisioned_numbers()
        
    
    def __enter__(self):
        return self
    
    def __exit__(self, type, value, tb):
        return False
    
    def add_current_values_to_obj(self):
        self.unprovisioned_number_df = (self.pull_current_spreadsheet_values_from_sheets(self.oauth_credentials))
    
    def compare_unique_with_known(self):
        return [phone_number_pair for phone_number_pair in self.unique_log_line_phone_numbers
                if (phone_number_pair[0], phone_number_pair[1]) not in self.known_unprovisioned_numbers]
    
    @property
    def log_line_string(self):
        return self.__log_line_string
    
    @log_line_string.setter
    def log_line_string(self, log_line_string):
        self.__log_line_string = log_line_string
        if any("NOMATCH" in log for log in log_line_string):
            p = re.compile(r"(NOMATCH,\d+,\d+,(?:\+|)(\d{1,}|\w+),(?:\+|)(\d{1,}|\w+))")
            
            # Run through list of logs in the Loki logline query. For each ref
            # of NOMATCH per log list item return each numer pair, account name,
            # and the full log line.
            self.unique_log_line_phone_numbers = set([(re.findall(p, log)[i][1], re.findall(p, log)[i][2],
                              re.findall(r"customerName=\"(.+)\"", log)[0],
                              re.findall(p, log)[i][0]) for log in log_line_string for i in range(len(re.findall(p, log)))])
            
        elif any("-->" in log for log in log_line_string):
            p = re.compile(r"((?:sourceNumber=\"|)(?:\+|)(\d{1,}|\w+)(?:.-->.|\".destinationNumber=\")"
                             r"(?:\+|)(\d{1,}|\w+)\"(?:.|)agencyId=\"(\d+)\")")
            
            log_line_data = [re.findall(p, log)[0] for log in log_line_string if re.findall(p, log)]
            
            self.unique_log_line_phone_numbers = set([(phone_number_tuple[1], phone_number_tuple[2],
                                                       phone_number_tuple[3], phone_number_tuple[0]
                                                      ) for phone_number_tuple in log_line_data])
