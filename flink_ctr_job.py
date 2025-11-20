import json
import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.udf import TableFunction, udtf

from config import BOOTSTRAP_SERVERS, TOPIC_NAME, CTR_RESULTS_TOPIC, WINDOW_SIZE_SEC, MAPPINGS_FILE

#config
PARALLELISM = 3

class CampaignMapper(TableFunction):
    def open(self, function_context):
        try:
            with open(MAPPINGS_FILE, 'r') as f:
                self.mappings = {ad_id: campaign_id for ad_id, campaign_id in json.load(f)}
        except Exception as e:
            print(f"ERROR loading mappings: {e}")
            self.mappings = {}

    def eval(self, ad_id):
        yield str(self.mappings.get(ad_id, 'UNKNOWN'))

def run_flink_job_append():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(PARALLELISM)
    t_env = StreamTableEnvironment.create(env)
    
    #FIX: Force Flink to use UTC for all timestamps 
    t_env.get_config().set_local_timezone("UTC")
    
    t_env.create_temporary_function(
        "CampaignMapper", 
        udtf(CampaignMapper(), result_types=[DataTypes.STRING()])
    )

    # Source Table
    t_env.execute_sql(f"""
        CREATE TABLE events_source (
            ad_id STRING,
            event_type STRING,
            event_time_ns BIGINT,
            event_time_ltz AS TO_TIMESTAMP_LTZ(event_time_ns / 1000000, 3),
            kafka_timestamp TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
            WATERMARK FOR event_time_ltz AS event_time_ltz - INTERVAL '5' SECOND 
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{TOPIC_NAME}',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    # Sink Table (Standard Kafka - APPEND ONLY)
    t_env.execute_sql(f"""
        CREATE TABLE ctr_sink (
            window_id INT,
            latest_produce_time STRING,
            latest_insert_time STRING,
            ctr_results STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{CTR_RESULTS_TOPIC}',
            'properties.bootstrap.servers' = '{BOOTSTRAP_SERVERS}',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    # Double-Window SQL Query
    sql_query = f"""
        SELECT
            CAST(CAST(UNIX_TIMESTAMP(CAST(TUMBLE_START(rowtime, INTERVAL '{WINDOW_SIZE_SEC}' SECOND) AS STRING)) AS BIGINT) / {WINDOW_SIZE_SEC} AS INT) AS window_id,
            
            DATE_FORMAT(MAX(max_kafka_time), 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''') AS latest_produce_time,
            
            -- This will now be in UTC because of set_local_timezone("UTC")
            DATE_FORMAT(CURRENT_TIMESTAMP, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''') AS latest_insert_time,
            
            JSON_ARRAYAGG(
                JSON_OBJECT(
                    KEY 'campaign_id' VALUE campaign_id,
                    KEY 'view_count' VALUE view_count,
                    KEY 'click_count' VALUE click_count,
                    KEY 'ctr' VALUE ctr
                )
            ) AS ctr_results
        FROM (
            SELECT
                TUMBLE_ROWTIME(event_time_ltz, INTERVAL '{WINDOW_SIZE_SEC}' SECOND) as rowtime,
                M.campaign_id,
                COUNT(CASE WHEN E.event_type = 'view' THEN 1 END) AS view_count,
                COUNT(CASE WHEN E.event_type = 'click' THEN 1 END) AS click_count,
                CAST(COUNT(CASE WHEN E.event_type = 'click' THEN 1 END) AS DOUBLE) / CAST(COUNT(CASE WHEN E.event_type = 'view' THEN 1 END) AS DOUBLE) AS ctr,
                MAX(E.kafka_timestamp) AS max_kafka_time
            FROM events_source E, LATERAL TABLE(CampaignMapper(E.ad_id)) AS M(campaign_id)
            GROUP BY TUMBLE(event_time_ltz, INTERVAL '{WINDOW_SIZE_SEC}' SECOND), M.campaign_id
        )
        GROUP BY TUMBLE(rowtime, INTERVAL '{WINDOW_SIZE_SEC}' SECOND)
    """
    
    print(f"Submitting Flink Job (Pure Append) with Parallelism: {PARALLELISM}")
    t_env.execute_sql(f"INSERT INTO ctr_sink {sql_query}")

if __name__ == '__main__':
    run_flink_job_append()













