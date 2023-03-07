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
import requests as r

class SlackWebHook:
    WEB_HOOK_URL = "<slack webhook>"
    
    @classmethod
    def post_message_to_channel(cls, text):
        r.post(cls.WEB_HOOK_URL, json={"text": f"{text}"})
