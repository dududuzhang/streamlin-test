import streamlit as st
from databricks import sql
from databricks.sdk.core import Config
from dotenv import load_dotenv
load_dotenv()  # 自动读取项目根目录的 .env 文件

cfg = Config()  # Set the DATABRICKS_HOST environment variable when running locally
# DATABRICKS_PATH = 'sql/protocolv1/o/3117460798135006/1210-042457-47i4jn6a'
DATABRICKS_PATH = 'sql/protocolv1/o/3292840790767374/1014-081556-mahjj0tn'

@st.cache_resource  # connection is cached
def get_connection(http_path):
    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )

def read_table(table_name, conn):
    with conn.cursor() as cursor:
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()

http_path_input = st.text_input(
    "Enter your Databricks HTTP Path:", placeholder=DATABRICKS_PATH, value=DATABRICKS_PATH
)

table_name = st.text_input(
    "Specify a :re[UC] table name:", placeholder="catalog.schema.table",value="amer_bi_dev.dim.dim_data_quality_check"
)

conn = get_connection(http_path_input)
df = read_table(table_name, conn)
edit_table = st.data_editor(df,
column_config={
    "is_suspend":st.column_config.SelectboxColumn(help="is suspend when job failed", required=True, options =["Y","N"] ,default="N"),
    "rule_id":st.column_config.NumberColumn(help="auto-generate, can not edit", disabled=True ,required=False)
},
disabled=["rule_id"]
)



def _update_table(conn, update_sql, records):
    try:
        st.info('Saving', icon="ℹ️")
        with conn.cursor() as cursor:
            cursor.executemany(update_sql, records)
        try:
            conn.commit()
        except Exception:
            # some connectors auto-commit; ignore if commit unsupported
            pass
        st.success(f"Updated {len(records)} rows")
    except Exception as e:
        st.error(f"Update failed: {e}")
        st.exception(e)


def save_to_table(conn):
    print("successed!")
    df_update = edit_table[["dqc_subject","is_suspend","is_message_notify", "is_email_notify", "email_content", "email_subscriber", "dqc_type", "dqx_path", "rule_desc", "rule_level", "table_source", "table_filter", "column_rule", "custom_sql", "compare_type", "threshold", "is_enable", "dwh_last_mdf_dtime", "rule_id"]]
    # df_update = edit_table[["dqc_subject", "rule_id"]]
    
    records = [tuple(row) for row in df_update.to_numpy()]
    
    update_sql = """
        UPDATE amer_bi_dev.dim.dim_data_quality_check
        SET dqc_subject = ?, is_suspend = ?, is_message_notify = ?, is_email_notify = ?, email_content = ?, email_subscriber = ?, dqc_type = ?, dqx_path = ?, rule_desc = ?, rule_level = ?, table_source = ?, table_filter = ?, column_rule = ?, custom_sql = ?, compare_type = ?, threshold = ?, is_enable = ?, dwh_last_mdf_dtime = ?
        WHERE rule_id = ?
    """

    # update_sql = """
    #     UPDATE amer_bi_dev.dim.dim_data_quality_check SET dqc_subject = ? WHERE rule_id = ?
    # """
    # _update_table(conn, update_sql, records)



if st.button("save"):
    with st.status("Saving data...", expanded=True):
        save_to_table(conn)

