import json
import requests
import pandas as pd

def fs_read_df(sheet_address, feishu_cfg):
    # 获取tenant_access_token
    feishu_url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"
    post_data = {"app_id":feishu_cfg.feishu_app_id,
                "app_secret":feishu_cfg.feishu_app_secret}
    r = requests.post(feishu_url, data=post_data)
    tat = r.json()["tenant_access_token"]
    header = {"content-type":"application/json",
    "Authorization":"Bearer " + str(tat)}
    url = "https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/" + sheet_address[0] + "/values/" + sheet_address[1] \
        + "?valueRenderOption=ToString&dateTimeRenderOption=FormattedString"
    r = requests.get(url, headers = header)
    pull_data = r.json()['data']['valueRange']['values']
    return pd.DataFrame(pull_data[1:], columns=pull_data[0])