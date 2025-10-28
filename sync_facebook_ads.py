from google.cloud import bigquery
import requests
from datetime import datetime
import concurrent.futures

LARK_APP_ID = "cli_a8620f964a38d02f" 
LARK_APP_SECRET = "G3FdlSvmTAXZYX8SBZtfpckHUiWUCO4h"   
LARK_APP_TOKEN = "WBgsbJc9uaRDnFsv7zrlrgRVgJr"

BQ_PROJECT = "atino-vietnam"
BQ_DATASET = "san_xuat"
BQ_TABLE = "facebook_ads"

BATCH_SIZE = 500
MAX_WORKERS = 5

TABLES = {
    "campaign": {
        "table_id": "tblpmkoXyZTGqiFC",
        "query": f"""
            SELECT
                campaign_link,
                campaign_name,
                STRING_AGG(DISTINCT mapped_store, ', ') AS mapped_store,
                SUM(impressions) AS impressions,
                AVG(SAFE_CAST(frequency AS FLOAT64)) AS frequency,
                SUM(reach) AS reach,
                SUM(clicks) AS clicks,
                SUM(omni_purchase_actions) AS omni_purchase_actions,
                SUM(omni_purchase) AS omni_purchase,
                SUM(spend) AS spend,
                SAFE_DIVIDE(SUM(spend), SUM(clicks)) AS cpc,
                SAFE_DIVIDE(SUM(spend) * 1000, SUM(impressions)) AS cpm,
                SAFE_DIVIDE(SUM(spend), SUM(reach)) AS cpp,
                SAFE_DIVIDE(SUM(clicks) * 100, SUM(impressions)) AS ctr
            FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
            GROUP BY campaign_link, campaign_name
            ORDER BY spend DESC
        """,
        "converter": lambda row: {
            "Tên chiến dịch": str(row.campaign_name or ""),
            "Link chiến dịch": str(row.campaign_link or ""),
            "Cửa hàng": str(row.mapped_store or ""),
            "Lượt hiển thị": int(row.impressions or 0),
            "Lượt tiếp cận": int(row.reach or 0),
            "Tần suất": float(row.frequency or 0),
            "Tổng click": int(row.clicks or 0),
            "Đơn Omni": int(row.omni_purchase_actions or 0),
            "Tổng chi tiêu": float(row.spend or 0),
            "CPC (Chi phí mỗi click)": float(row.cpc or 0),
            "CPM (Chi phí 1000 hiển thị)": float(row.cpm or 0),
            "CPP (Chi phí mỗi tiếp cận)": float(row.cpp or 0),
            "CTR (Tỷ lệ click)": float(row.ctr or 0),
            "Doanh thu Omni": float(row.omni_purchase or 0),
        }
    },
    "ad_set": {
        "table_id": "tblwrFW6zMx9uWVC",
        "query": f"""
            SELECT
                adset_id,
                adset_name,
                STRING_AGG(DISTINCT mapped_store, ', ') AS mapped_store,
                SUM(impressions) AS impressions,
                AVG(SAFE_CAST(frequency AS FLOAT64)) AS frequency,
                SUM(reach) AS reach,
                SUM(clicks) AS clicks,
                SUM(omni_purchase_actions) AS omni_purchase_actions,
                SUM(omni_purchase) AS omni_purchase,
                SUM(spend) AS spend,
                SAFE_DIVIDE(SUM(spend), SUM(clicks)) AS cpc,
                SAFE_DIVIDE(SUM(spend) * 1000, SUM(impressions)) AS cpm,
                SAFE_DIVIDE(SUM(spend), SUM(reach)) AS cpp,
                SAFE_DIVIDE(SUM(clicks) * 100, SUM(impressions)) AS ctr
            FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
            GROUP BY adset_id, adset_name
            ORDER BY spend DESC
        """,
        "converter": lambda row: {
            "Ad Set ID": str(row.adset_id or ""),
            "Ad Set Name": str(row.adset_name or ""),
            "Cửa hàng": str(row.mapped_store or ""),
            "Lượt hiển thị": int(row.impressions or 0),
            "Lượt tiếp cận": int(row.reach or 0),
            "Tần suất": float(row.frequency or 0),
            "Tổng click": int(row.clicks or 0),
            "Đơn Omni": int(row.omni_purchase_actions or 0),
            "Tổng chi tiêu": float(row.spend or 0),
            "CPC (Chi phí mỗi click)": float(row.cpc or 0),
            "CPM (Chi phí 1000 hiển thị)": float(row.cpm or 0),
            "CPP (Chi phí mỗi tiếp cận)": float(row.cpp or 0),
            "CTR (Tỷ lệ click)": float(row.ctr or 0),
            "Doanh thu Omni": float(row.omni_purchase or 0),
        }
    },
    "ad": {
        "table_id": "tblMBbngnpJvEqbL",
        "query": f"""
            SELECT
                ad_id,
                ad_name,
                STRING_AGG(DISTINCT mapped_store, ', ') AS mapped_store,
                SUM(impressions) AS impressions,
                AVG(SAFE_CAST(frequency AS FLOAT64)) AS frequency,
                SUM(reach) AS reach,
                SUM(clicks) AS clicks,
                SUM(omni_purchase_actions) AS omni_purchase_actions,
                SUM(omni_purchase) AS omni_purchase,
                SUM(spend) AS spend,
                SAFE_DIVIDE(SUM(spend), SUM(clicks)) AS cpc,
                SAFE_DIVIDE(SUM(spend) * 1000, SUM(impressions)) AS cpm,
                SAFE_DIVIDE(SUM(spend), SUM(reach)) AS cpp,
                SAFE_DIVIDE(SUM(clicks) * 100, SUM(impressions)) AS ctr
            FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
            GROUP BY ad_id, ad_name
            ORDER BY spend DESC
        """,
        "converter": lambda row: {
            "Ad ID": str(row.ad_id or ""),
            "Ad Name": str(row.ad_name or ""),
            "Cửa hàng": str(row.mapped_store or ""),
            "Lượt hiển thị": int(row.impressions or 0),
            "Lượt tiếp cận": int(row.reach or 0),
            "Tần suất": float(row.frequency or 0),
            "Tổng click": int(row.clicks or 0),
            "Đơn Omni": int(row.omni_purchase_actions or 0),
            "Tổng chi tiêu": float(row.spend or 0),
            "CPC (Chi phí mỗi click)": float(row.cpc or 0),
            "CPM (Chi phí 1000 hiển thị)": float(row.cpm or 0),
            "CPP (Chi phí mỗi tiếp cận)": float(row.cpp or 0),
            "CTR (Tỷ lệ click)": float(row.ctr or 0),
            "Doanh thu Omni": float(row.omni_purchase or 0),
        }
    },
    "daily": {
        "table_id": "tblZEomjtx2J2xVr",
        "query": f"""
            SELECT *
            FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
            WHERE date_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            ORDER BY date_start DESC
        """,
        "converter": lambda row: {
            "Tên chiến dịch": str(row.campaign_name or ""),
            "Link chiến dịch": str(row.campaign_link or ""),
            "mapped_store": str(row.mapped_store or ""),
            "adset_name": str(row.adset_name or ""),
            "ad_name": str(row.ad_name or ""),
            "adset_id": str(row.adset_id or ""),
            "ad_id": str(row.ad_id or ""),
            "Lượt hiển thị": int(row.impressions or 0),
            "Lượt tiếp cận": int(row.reach or 0),
            "Tổng click": int(row.clicks or 0),
            "Đơn Omni": int(row.omni_purchase_actions or 0),
            "Tổng chi tiêu": float(row.spend or 0),
            "CPC (Chi phí mỗi click)": float(row.cpc or 0),
            "CPM (Chi phí 1000 hiển thị)": float(row.cpm or 0),
            "CPP (Chi phí mỗi tiếp cận)": float(row.cpp or 0),
            "CTR (Tỷ lệ click)": float(row.ctr or 0),
            "Doanh thu Omni": float(row.omni_purchase or 0),
            "Tần suất": float(row.frequency or 0),
            "Ngày bắt đầu": int(datetime.strptime(str(row.date_start), '%Y-%m-%d').timestamp() * 1000) if row.date_start else None,
            "Ngày kết thúc": int(datetime.strptime(str(row.date_stop), '%Y-%m-%d').timestamp() * 1000) if row.date_stop else None,
        }
    },
    "ad_accounts": {
        "table_id": "tblhkPef4KZKdUyp",
        "query": f"""
            SELECT 
                account_id, 
                name, 
                currency, 
                balance, 
                account_status, 
                amount_spent 
            FROM `{BQ_PROJECT}.san_xuat.ad_accounts`
            LIMIT 1000
        """,
        "converter": lambda row: {
            "Account ID": str(row.account_id or ""),
            "Name": str(row.name or ""),
            "Currency": str(row.currency or ""),
            "Balance": float(row.balance or 0),
            "Account Status": str(row.account_status or ""),
            "Amount Spent": float(row.amount_spent or 0),
        },
        "key_field": "Account ID",
        "update_mode": True
    }
}

def get_lark_token(app_id, app_secret):
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    response = requests.post(url, headers={"Content-Type": "application/json"}, json={"app_id": app_id, "app_secret": app_secret})
    result = response.json()
    return result["tenant_access_token"] if result.get("code") == 0 else None

def get_existing_records(token, app_token, table_id, key_field):
    """Get all existing records and build a map of key_field -> record_id"""
    list_url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    existing_map = {}
    page_token = None
    
    while True:
        params = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token
        
        response = requests.get(list_url, headers=headers, params=params)
        result = response.json()
        
        if result.get("code") != 0:
            print(f"Warning: API returned code {result.get('code')}: {result.get('msg', 'Unknown error')}")
            break
        
        items = result.get("data", {}).get("items")
        if items is None:
            items = []
        
        for item in items:
            key_value = item.get("fields", {}).get(key_field)
            if key_value:
                existing_map[str(key_value)] = item.get("record_id")
        
        page_token = result.get("data", {}).get("page_token")
        if not page_token or not result.get("data", {}).get("has_more", False):
            break
    
    return existing_map

def upsert_to_lark_base(token, app_token, table_id, records, converter, key_field):
    """Update existing records or create new ones based on key_field"""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    if len(records) == 0:
        return 0, 0
    
    print("Fetching existing records...")
    existing_map = get_existing_records(token, app_token, table_id, key_field)
    print(f"Found {len(existing_map)} existing records")
    
    records_to_create = []
    records_to_update = []
    
    for row in records:
        try:
            fields = converter(row)
            fields = {k: v for k, v in fields.items() if v is not None}
            key_value = str(fields.get(key_field, ""))
            
            if key_value in existing_map:
                records_to_update.append({
                    "record_id": existing_map[key_value],
                    "fields": fields
                })
            else:
                records_to_create.append({"fields": fields})
        except:
            continue
    
    created_count = 0
    updated_count = 0
    
    # Create new records
    if records_to_create:
        create_url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
        
        def create_batch(batch):
            response = requests.post(create_url, headers=headers, json={"records": batch})
            return len(batch) if response.json().get("code") == 0 else 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            batches = [records_to_create[i:i+BATCH_SIZE] for i in range(0, len(records_to_create), BATCH_SIZE)]
            results = executor.map(create_batch, batches)
            created_count = sum(results)
    
    # Update existing records
    if records_to_update:
        update_url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update"
        
        def update_batch(batch):
            response = requests.post(update_url, headers=headers, json={"records": batch})
            return len(batch) if response.json().get("code") == 0 else 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            batches = [records_to_update[i:i+BATCH_SIZE] for i in range(0, len(records_to_update), BATCH_SIZE)]
            results = executor.map(update_batch, batches)
            updated_count = sum(results)
    
    return created_count, updated_count

def delete_all_records(token, app_token, table_id):
    list_url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    all_record_ids = []
    page_token = None
    
    while True:
        params = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token
        
        response = requests.get(list_url, headers=headers, params=params)
        result = response.json()
        
        if result.get("code") != 0:
            break
        
        items = result.get("data", {}).get("items", [])
        all_record_ids.extend([item.get("record_id") for item in items])
        
        page_token = result.get("data", {}).get("page_token")
        if not page_token or not result.get("data", {}).get("has_more", False):
            break
    
    if len(all_record_ids) == 0:
        return 0
    
    delete_url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_delete"
    
    def delete_batch(batch):
        response = requests.post(delete_url, headers=headers, json={"records": batch})
        return len(batch) if response.json().get("code") == 0 else 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        batches = [all_record_ids[i:i+BATCH_SIZE] for i in range(0, len(all_record_ids), BATCH_SIZE)]
        results = executor.map(delete_batch, batches)
        return sum(results)

def write_to_lark_base(token, app_token, table_id, records, converter):
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    if len(records) == 0:
        return 0
    
    lark_records = []
    for row in records:
        try:
            fields = converter(row)
            fields = {k: v for k, v in fields.items() if v is not None}
            lark_records.append({"fields": fields})
        except:
            continue
    
    def create_batch(batch):
        response = requests.post(url, headers=headers, json={"records": batch})
        return len(batch) if response.json().get("code") == 0 else 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        batches = [lark_records[i:i+BATCH_SIZE] for i in range(0, len(lark_records), BATCH_SIZE)]
        results = executor.map(create_batch, batches)
        return sum(results)

def sync_table(name, config, client, token):
    print(f"\n{'='*60}")
    print(f"SYNCING: {name.upper()}")
    print(f"{'='*60}")
    
    print("Querying BigQuery...")
    try:
        query_job = client.query(config["query"])
        results = list(query_job.result())
        print(f"Fetched {len(results)} records")
    except Exception as e:
        print(f"Error: {e}")
        return
    
    if len(results) == 0:
        print("No data found")
        return
    
    # Check if this table uses update mode
    if config.get("update_mode") == True and config.get("key_field"):
        key_field = config.get("key_field")
        print(f"Using UPDATE/CREATE mode with key field: {key_field}")
        created, updated = upsert_to_lark_base(token, LARK_APP_TOKEN, config["table_id"], results, config["converter"], key_field)
        print(f"Created: {created}, Updated: {updated}/{len(results)} records")
    else:
        print("Using DELETE & RECREATE mode")
        print("Deleting old data...")
        deleted = delete_all_records(token, LARK_APP_TOKEN, config["table_id"])
        print(f"Deleted {deleted} records")
        
        print("Creating new data...")
        created = write_to_lark_base(token, LARK_APP_TOKEN, config["table_id"], results, config["converter"])
        print(f"Created {created}/{len(results)} records")

print("="*60)
print("FACEBOOK ADS SYNC - ALL TABLES")
print("="*60)

print("\nConnecting to BigQuery...")
client = bigquery.Client(project=BQ_PROJECT)

print("Connecting to Lark Base...")
token = get_lark_token(LARK_APP_ID, LARK_APP_SECRET)

if not token:
    print("Failed to get Lark token")
    exit(1)

print("Connected successfully")

start_time = datetime.now()

for name, config in TABLES.items():
    sync_table(name, config, client, token)

end_time = datetime.now()
duration = (end_time - start_time).total_seconds()

print(f"\n{'='*60}")
print("ALL TABLES COMPLETED!")
print(f"{'='*60}")
print(f"Total duration: {duration:.2f}s ({duration/60:.2f}min)")
print(f"Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*60}")from google.cloud import bigquery
import requests
from datetime import datetime
import concurrent.futures

LARK_APP_ID = "cli_a8620f964a38d02f" 
LARK_APP_SECRET = "G3FdlSvmTAXZYX8SBZtfpckHUiWUCO4h"   
LARK_APP_TOKEN = "WBgsbJc9uaRDnFsv7zrlrgRVgJr"

BQ_PROJECT = "atino-vietnam"
BQ_DATASET = "san_xuat"
BQ_TABLE = "facebook_ads"

BATCH_SIZE = 500
MAX_WORKERS = 5

TABLES = {
    "campaign": {
        "table_id": "tblpmkoXyZTGqiFC",
        "query": f"""
            SELECT
                campaign_link,
                campaign_name,
                STRING_AGG(DISTINCT mapped_store, ', ') AS mapped_store,
                SUM(impressions) AS impressions,
                AVG(SAFE_CAST(frequency AS FLOAT64)) AS frequency,
                SUM(reach) AS reach,
                SUM(clicks) AS clicks,
                SUM(omni_purchase_actions) AS omni_purchase_actions,
                SUM(omni_purchase) AS omni_purchase,
                SUM(spend) AS spend,
                SAFE_DIVIDE(SUM(spend), SUM(clicks)) AS cpc,
                SAFE_DIVIDE(SUM(spend) * 1000, SUM(impressions)) AS cpm,
                SAFE_DIVIDE(SUM(spend), SUM(reach)) AS cpp,
                SAFE_DIVIDE(SUM(clicks) * 100, SUM(impressions)) AS ctr
            FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
            GROUP BY campaign_link, campaign_name
            ORDER BY spend DESC
        """,
        "converter": lambda row: {
            "Tên chiến dịch": str(row.campaign_name or ""),
            "Link chiến dịch": str(row.campaign_link or ""),
            "Cửa hàng": str(row.mapped_store or ""),
            "Lượt hiển thị": int(row.impressions or 0),
            "Lượt tiếp cận": int(row.reach or 0),
            "Tần suất": float(row.frequency or 0),
            "Tổng click": int(row.clicks or 0),
            "Đơn Omni": int(row.omni_purchase_actions or 0),
            "Tổng chi tiêu": float(row.spend or 0),
            "CPC (Chi phí mỗi click)": float(row.cpc or 0),
            "CPM (Chi phí 1000 hiển thị)": float(row.cpm or 0),
            "CPP (Chi phí mỗi tiếp cận)": float(row.cpp or 0),
            "CTR (Tỷ lệ click)": float(row.ctr or 0),
            "Doanh thu Omni": float(row.omni_purchase or 0),
        }
    },
    "ad_set": {
        "table_id": "tblwrFW6zMx9uWVC",
        "query": f"""
            SELECT
                adset_id,
                adset_name,
                STRING_AGG(DISTINCT mapped_store, ', ') AS mapped_store,
                SUM(impressions) AS impressions,
                AVG(SAFE_CAST(frequency AS FLOAT64)) AS frequency,
                SUM(reach) AS reach,
                SUM(clicks) AS clicks,
                SUM(omni_purchase_actions) AS omni_purchase_actions,
                SUM(omni_purchase) AS omni_purchase,
                SUM(spend) AS spend,
                SAFE_DIVIDE(SUM(spend), SUM(clicks)) AS cpc,
                SAFE_DIVIDE(SUM(spend) * 1000, SUM(impressions)) AS cpm,
                SAFE_DIVIDE(SUM(spend), SUM(reach)) AS cpp,
                SAFE_DIVIDE(SUM(clicks) * 100, SUM(impressions)) AS ctr
            FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
            GROUP BY adset_id, adset_name
            ORDER BY spend DESC
        """,
        "converter": lambda row: {
            "Ad Set ID": str(row.adset_id or ""),
            "Ad Set Name": str(row.adset_name or ""),
            "Cửa hàng": str(row.mapped_store or ""),
            "Lượt hiển thị": int(row.impressions or 0),
            "Lượt tiếp cận": int(row.reach or 0),
            "Tần suất": float(row.frequency or 0),
            "Tổng click": int(row.clicks or 0),
            "Đơn Omni": int(row.omni_purchase_actions or 0),
            "Tổng chi tiêu": float(row.spend or 0),
            "CPC (Chi phí mỗi click)": float(row.cpc or 0),
            "CPM (Chi phí 1000 hiển thị)": float(row.cpm or 0),
            "CPP (Chi phí mỗi tiếp cận)": float(row.cpp or 0),
            "CTR (Tỷ lệ click)": float(row.ctr or 0),
            "Doanh thu Omni": float(row.omni_purchase or 0),
        }
    },
    "ad": {
        "table_id": "tblMBbngnpJvEqbL",
        "query": f"""
            SELECT
                ad_id,
                ad_name,
                STRING_AGG(DISTINCT mapped_store, ', ') AS mapped_store,
                SUM(impressions) AS impressions,
                AVG(SAFE_CAST(frequency AS FLOAT64)) AS frequency,
                SUM(reach) AS reach,
                SUM(clicks) AS clicks,
                SUM(omni_purchase_actions) AS omni_purchase_actions,
                SUM(omni_purchase) AS omni_purchase,
                SUM(spend) AS spend,
                SAFE_DIVIDE(SUM(spend), SUM(clicks)) AS cpc,
                SAFE_DIVIDE(SUM(spend) * 1000, SUM(impressions)) AS cpm,
                SAFE_DIVIDE(SUM(spend), SUM(reach)) AS cpp,
                SAFE_DIVIDE(SUM(clicks) * 100, SUM(impressions)) AS ctr
            FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
            GROUP BY ad_id, ad_name
            ORDER BY spend DESC
        """,
        "converter": lambda row: {
            "Ad ID": str(row.ad_id or ""),
            "Ad Name": str(row.ad_name or ""),
            "Cửa hàng": str(row.mapped_store or ""),
            "Lượt hiển thị": int(row.impressions or 0),
            "Lượt tiếp cận": int(row.reach or 0),
            "Tần suất": float(row.frequency or 0),
            "Tổng click": int(row.clicks or 0),
            "Đơn Omni": int(row.omni_purchase_actions or 0),
            "Tổng chi tiêu": float(row.spend or 0),
            "CPC (Chi phí mỗi click)": float(row.cpc or 0),
            "CPM (Chi phí 1000 hiển thị)": float(row.cpm or 0),
            "CPP (Chi phí mỗi tiếp cận)": float(row.cpp or 0),
            "CTR (Tỷ lệ click)": float(row.ctr or 0),
            "Doanh thu Omni": float(row.omni_purchase or 0),
        }
    },
    "daily": {
        "table_id": "tblZEomjtx2J2xVr",
        "query": f"""
            SELECT *
            FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`
            WHERE date_start >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            ORDER BY date_start DESC
        """,
        "converter": lambda row: {
            "Tên chiến dịch": str(row.campaign_name or ""),
            "Link chiến dịch": str(row.campaign_link or ""),
            "mapped_store": str(row.mapped_store or ""),
            "adset_name": str(row.adset_name or ""),
            "ad_name": str(row.ad_name or ""),
            "adset_id": str(row.adset_id or ""),
            "ad_id": str(row.ad_id or ""),
            "Lượt hiển thị": int(row.impressions or 0),
            "Lượt tiếp cận": int(row.reach or 0),
            "Tổng click": int(row.clicks or 0),
            "Đơn Omni": int(row.omni_purchase_actions or 0),
            "Tổng chi tiêu": float(row.spend or 0),
            "CPC (Chi phí mỗi click)": float(row.cpc or 0),
            "CPM (Chi phí 1000 hiển thị)": float(row.cpm or 0),
            "CPP (Chi phí mỗi tiếp cận)": float(row.cpp or 0),
            "CTR (Tỷ lệ click)": float(row.ctr or 0),
            "Doanh thu Omni": float(row.omni_purchase or 0),
            "Tần suất": float(row.frequency or 0),
            "Ngày bắt đầu": int(datetime.strptime(str(row.date_start), '%Y-%m-%d').timestamp() * 1000) if row.date_start else None,
            "Ngày kết thúc": int(datetime.strptime(str(row.date_stop), '%Y-%m-%d').timestamp() * 1000) if row.date_stop else None,
        }
    },
    "ad_accounts": {
        "table_id": "tblhkPef4KZKdUyp",
        "query": f"""
            SELECT 
                account_id, 
                name, 
                currency, 
                balance, 
                account_status, 
                amount_spent 
            FROM `{BQ_PROJECT}.san_xuat.ad_accounts`
            LIMIT 1000
        """,
        "converter": lambda row: {
            "Account ID": str(row.account_id or ""),
            "Name": str(row.name or ""),
            "Currency": str(row.currency or ""),
            "Balance": float(row.balance or 0),
            "Account Status": str(row.account_status or ""),
            "Amount Spent": float(row.amount_spent or 0),
        },
        "key_field": "Account ID",
        "update_mode": True
    }
}

def get_lark_token(app_id, app_secret):
    url = "https://open.larksuite.com/open-apis/auth/v3/tenant_access_token/internal"
    response = requests.post(url, headers={"Content-Type": "application/json"}, json={"app_id": app_id, "app_secret": app_secret})
    result = response.json()
    return result["tenant_access_token"] if result.get("code") == 0 else None

def get_existing_records(token, app_token, table_id, key_field):
    """Get all existing records and build a map of key_field -> record_id"""
    list_url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    existing_map = {}
    page_token = None
    
    while True:
        params = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token
        
        response = requests.get(list_url, headers=headers, params=params)
        result = response.json()
        
        if result.get("code") != 0:
            print(f"Warning: API returned code {result.get('code')}: {result.get('msg', 'Unknown error')}")
            break
        
        items = result.get("data", {}).get("items")
        if items is None:
            items = []
        
        for item in items:
            key_value = item.get("fields", {}).get(key_field)
            if key_value:
                existing_map[str(key_value)] = item.get("record_id")
        
        page_token = result.get("data", {}).get("page_token")
        if not page_token or not result.get("data", {}).get("has_more", False):
            break
    
    return existing_map

def upsert_to_lark_base(token, app_token, table_id, records, converter, key_field):
    """Update existing records or create new ones based on key_field"""
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    if len(records) == 0:
        return 0, 0
    
    print("Fetching existing records...")
    existing_map = get_existing_records(token, app_token, table_id, key_field)
    print(f"Found {len(existing_map)} existing records")
    
    records_to_create = []
    records_to_update = []
    
    for row in records:
        try:
            fields = converter(row)
            fields = {k: v for k, v in fields.items() if v is not None}
            key_value = str(fields.get(key_field, ""))
            
            if key_value in existing_map:
                records_to_update.append({
                    "record_id": existing_map[key_value],
                    "fields": fields
                })
            else:
                records_to_create.append({"fields": fields})
        except:
            continue
    
    created_count = 0
    updated_count = 0
    
    # Create new records
    if records_to_create:
        create_url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
        
        def create_batch(batch):
            response = requests.post(create_url, headers=headers, json={"records": batch})
            return len(batch) if response.json().get("code") == 0 else 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            batches = [records_to_create[i:i+BATCH_SIZE] for i in range(0, len(records_to_create), BATCH_SIZE)]
            results = executor.map(create_batch, batches)
            created_count = sum(results)
    
    # Update existing records
    if records_to_update:
        update_url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_update"
        
        def update_batch(batch):
            response = requests.post(update_url, headers=headers, json={"records": batch})
            return len(batch) if response.json().get("code") == 0 else 0
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            batches = [records_to_update[i:i+BATCH_SIZE] for i in range(0, len(records_to_update), BATCH_SIZE)]
            results = executor.map(update_batch, batches)
            updated_count = sum(results)
    
    return created_count, updated_count

def delete_all_records(token, app_token, table_id):
    list_url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    all_record_ids = []
    page_token = None
    
    while True:
        params = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token
        
        response = requests.get(list_url, headers=headers, params=params)
        result = response.json()
        
        if result.get("code") != 0:
            break
        
        items = result.get("data", {}).get("items", [])
        all_record_ids.extend([item.get("record_id") for item in items])
        
        page_token = result.get("data", {}).get("page_token")
        if not page_token or not result.get("data", {}).get("has_more", False):
            break
    
    if len(all_record_ids) == 0:
        return 0
    
    delete_url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_delete"
    
    def delete_batch(batch):
        response = requests.post(delete_url, headers=headers, json={"records": batch})
        return len(batch) if response.json().get("code") == 0 else 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        batches = [all_record_ids[i:i+BATCH_SIZE] for i in range(0, len(all_record_ids), BATCH_SIZE)]
        results = executor.map(delete_batch, batches)
        return sum(results)

def write_to_lark_base(token, app_token, table_id, records, converter):
    url = f"https://open.larksuite.com/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/batch_create"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    
    if len(records) == 0:
        return 0
    
    lark_records = []
    for row in records:
        try:
            fields = converter(row)
            fields = {k: v for k, v in fields.items() if v is not None}
            lark_records.append({"fields": fields})
        except:
            continue
    
    def create_batch(batch):
        response = requests.post(url, headers=headers, json={"records": batch})
        return len(batch) if response.json().get("code") == 0 else 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        batches = [lark_records[i:i+BATCH_SIZE] for i in range(0, len(lark_records), BATCH_SIZE)]
        results = executor.map(create_batch, batches)
        return sum(results)

def sync_table(name, config, client, token):
    print(f"\n{'='*60}")
    print(f"SYNCING: {name.upper()}")
    print(f"{'='*60}")
    
    print("Querying BigQuery...")
    try:
        query_job = client.query(config["query"])
        results = list(query_job.result())
        print(f"Fetched {len(results)} records")
    except Exception as e:
        print(f"Error: {e}")
        return
    
    if len(results) == 0:
        print("No data found")
        return
    
    # Check if this table uses update mode
    if config.get("update_mode") == True and config.get("key_field"):
        key_field = config.get("key_field")
        print(f"Using UPDATE/CREATE mode with key field: {key_field}")
        created, updated = upsert_to_lark_base(token, LARK_APP_TOKEN, config["table_id"], results, config["converter"], key_field)
        print(f"Created: {created}, Updated: {updated}/{len(results)} records")
    else:
        print("Using DELETE & RECREATE mode")
        print("Deleting old data...")
        deleted = delete_all_records(token, LARK_APP_TOKEN, config["table_id"])
        print(f"Deleted {deleted} records")
        
        print("Creating new data...")
        created = write_to_lark_base(token, LARK_APP_TOKEN, config["table_id"], results, config["converter"])
        print(f"Created {created}/{len(results)} records")

print("="*60)
print("FACEBOOK ADS SYNC - ALL TABLES")
print("="*60)

print("\nConnecting to BigQuery...")
client = bigquery.Client(project=BQ_PROJECT)

print("Connecting to Lark Base...")
token = get_lark_token(LARK_APP_ID, LARK_APP_SECRET)

if not token:
    print("Failed to get Lark token")
    exit(1)

print("Connected successfully")

start_time = datetime.now()

for name, config in TABLES.items():
    sync_table(name, config, client, token)

end_time = datetime.now()
duration = (end_time - start_time).total_seconds()

print(f"\n{'='*60}")
print("ALL TABLES COMPLETED!")
print(f"{'='*60}")
print(f"Total duration: {duration:.2f}s ({duration/60:.2f}min)")
print(f"Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'='*60}")
