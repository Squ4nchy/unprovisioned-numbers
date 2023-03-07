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
import json
import requests as r
import time

import pandas as pd

class LokiApiInteraction:
    '''
    Handle interactions with the Loki API to return search queries.
    '''
    
    query_url = "http://loki.<internal url>/loki/api/v1/query_range"
    
    source_process_dict = {
        "OracleIsrFaceSync": {
            "no_match_query_term": " |= `NOMATCH`",
            "numbers_to_search" : "{0},+{1}"
        },
        "OpenCloudSmsSync": {
            "no_match_query_term": "",
            "numbers_to_search" : "{0} --> +{1}"
        }
    }
    
    def __init__(self, source_process):
        self.source_process = source_process
    
    @staticmethod
    def get_query_log_values(json_text):
        '''
        Extract the values from json which is returned from a Loki query, targetting the "values" keyword.
        Format of list of lists.
        '''
        
        results = []

        def _decode_dict(a_dict):
            try:
                results.append(a_dict['values'])
            except KeyError:
                pass

            return a_dict
        
        json.loads(json_text, object_hook=_decode_dict)
        return results
    
    def query_loki_for_error_log_line(self):
        '''
        Queries Loki based on the values stored in source_process_dict, dependent on which tenant
        is being queried.
        '''
        
        now = time.time()
        query_parameters = {
            "query": (
                f'{{SourceProcess="<internal ref>.{self.source_process}.<internal ref>"'
                f',Level="Error"}}{self.source_process_dict[self.source_process]["no_match_query_term"]}'
            ),
            "limit": 50,
            "start": now - (60 * 60 * 5),
            "end": now
        }
        
        query_response_values = self.get_query_log_values(r.get(self.query_url, params=query_parameters).text)
        
        # Return a list second values for each nested list. First value is null   
        return [indv_list[1] for lists in query_response_values for indv_list in lists]
    
    
    def query_loki_confirm_provisioned_numbers(self):
        '''
        Given a list of confirmed un-provisioned numbers, search Loki for them to confirm whether
        they have been provisioned yet.
        '''
        
        provisioned_numbers = pd.DataFrame()
        
        for num_pair in self.known_unprovisioned_numbers:
            now = time.time()
            query_parameters = {
                    "query": (
                        f'{{SourceProcess="<internal ref>.{self.source_process}.<internal ref>", '
                        f'Level="Error"}}{self.source_process_dict[self.source_process]["no_match_query_term"]} |= `'
                        f'{self.source_process_dict[self.source_process]["numbers_to_search"].format(num_pair[0], num_pair[1])}`'
                    ),
                    "limit": 50,
                    "start": now - (60 * 5),
                    "end": now
                }
            
            query_response = self.get_query_log_values(r.get(self.query_url, params=query_parameters).text)
            
            if len(query_response) == 0:
                provisioned_numbers = provisioned_numbers.append(
                    self.filtered_df[
                        (self.filtered_df['from'] == num_pair[0]) &
                        (self.filtered_df['to'].astype(str) == num_pair[1])
                    ]
                )
        
        return provisioned_numbers
    
    @property
    def source_process(self):
        return self.__source_process
    
    @source_process.setter
    def source_process(self, source_process):
        if source_process not in ("<process 1>", "<process 2>"):
            raise Exception("Only '<process 1>' and '<process 2>' are valid inputs.")
        
        self.__source_process = source_process
