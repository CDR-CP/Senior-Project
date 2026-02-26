import duckdb

# Path to parquet files
VOLTAGE_FILE = "parquet_out\\battery_voltage_v.parquet"
CURRENT_FILE = "parquet_out\\battery_current_a.parquet"

def main():
    con = duckdb.connect()

    # Create views over the parquet files
    con.execute(f"""
        CREATE OR REPLACE VIEW voltage AS
        SELECT timestamp_utc, seq, run_id, battery_voltage_v
        FROM read_parquet('{VOLTAGE_FILE}')
    """)
 
    con.execute(f"""
        CREATE OR REPLACE VIEW current AS
        SELECT timestamp_utc, seq, run_id, battery_current_a
        FROM read_parquet('{CURRENT_FILE}')
    """)

    # Join voltage and current on run_id and seq (or timestamp if more reliable)
    # Compute instantaneous power (W = V * A)
    # Compute time delta between rows
    # Integrate power over time to get energy in watt-hours

    query = """
    WITH joined AS (
        SELECT
            v.run_id,
            v.timestamp_utc,
            v.seq,
            v.battery_voltage_v,
            c.battery_current_a,
            (v.battery_voltage_v * c.battery_current_a) AS power_w
        FROM voltage v
        JOIN current c
            ON v.run_id = c.run_id
           AND v.seq = c.seq
    ),
    with_deltas AS (
        SELECT
            run_id,
            timestamp_utc,
            power_w,
            EXTRACT(EPOCH FROM (timestamp_utc 
                - LAG(timestamp_utc) OVER (
                    PARTITION BY run_id 
                    ORDER BY timestamp_utc
                )
            )) AS delta_seconds
        FROM joined
    )
    SELECT
        run_id,
        SUM(power_w * delta_seconds) / 3600.0 AS energy_wh
    FROM with_deltas
    WHERE delta_seconds IS NOT NULL
    GROUP BY run_id
    ORDER BY run_id
    """

    result = con.execute(query).fetchdf()
    print(result)


if __name__ == "__main__":
    main()
