import duckdb

AX_FILE = "parquet_out\\imu_ax_mps2.parquet"
AY_FILE = "parquet_out\\imu_ay_mps2.parquet"
AZ_FILE = "parquet_out\\imu_az_mps2.parquet"

ROLLING_WINDOW = 10        # number of samples in rolling window
Z_SCORE_THRESHOLD = 3.0    # spike threshold (standard deviations)

def main():
    con = duckdb.connect()

    # Create views
    con.execute(f"""
        CREATE OR REPLACE VIEW ax AS
        SELECT
            CAST(timestamp_utc AS TIMESTAMP) AS timestamp_utc,
            seq,
            run_id,
            imu_ax_mps2
        FROM read_parquet('{AX_FILE}')
    """)

    con.execute(f"""
        CREATE OR REPLACE VIEW ay AS
        SELECT
            CAST(timestamp_utc AS TIMESTAMP) AS timestamp_utc,
            seq,
            run_id,
            imu_ay_mps2
        FROM read_parquet('{AY_FILE}')
    """)

    con.execute(f"""
        CREATE OR REPLACE VIEW az AS
        SELECT
            CAST(timestamp_utc AS TIMESTAMP) AS timestamp_utc,
            seq,
            run_id,
            imu_az_mps2
        FROM read_parquet('{AZ_FILE}')
    """)

    query = f"""
    WITH joined AS (
        SELECT
            ax.run_id,
            ax.timestamp_utc,
            ax.seq,
            ax.imu_ax_mps2,
            ay.imu_ay_mps2,
            az.imu_az_mps2
        FROM ax
        JOIN ay
            ON ax.run_id = ay.run_id
           AND ax.seq = ay.seq
        JOIN az
            ON ax.run_id = az.run_id
           AND ax.seq = az.seq
    ),
    magnitude AS (
        SELECT
            run_id,
            timestamp_utc,
            seq,
            SQRT(
                imu_ax_mps2 * imu_ax_mps2 +
                imu_ay_mps2 * imu_ay_mps2 +
                imu_az_mps2 * imu_az_mps2
            ) AS accel_mag
        FROM joined
    ),
    rolling_stats AS (
        SELECT
            run_id,
            timestamp_utc,
            seq,
            accel_mag,
            AVG(accel_mag) OVER (
                PARTITION BY run_id
                ORDER BY timestamp_utc
                ROWS BETWEEN {ROLLING_WINDOW} PRECEDING AND CURRENT ROW
            ) AS rolling_mean,
            STDDEV_SAMP(accel_mag) OVER (
                PARTITION BY run_id
                ORDER BY timestamp_utc
                ROWS BETWEEN {ROLLING_WINDOW} PRECEDING AND CURRENT ROW
            ) AS rolling_std
        FROM magnitude
    ),
    scored AS (
        SELECT
            run_id,
            timestamp_utc,
            seq,
            accel_mag,
            rolling_mean,
            rolling_std,
            CASE
                WHEN rolling_std > 0
                THEN (accel_mag - rolling_mean) / rolling_std
                ELSE 0
            END AS z_score
        FROM rolling_stats
    )
    SELECT *
    FROM scored
    WHERE ABS(z_score) >= {Z_SCORE_THRESHOLD}
    ORDER BY run_id, timestamp_utc
    """

    anomalies = con.execute(query).fetchdf()
    
    if anomalies.empty:
        print("No motion anomalies detected.")
    else:
        print(f"Detected {len(anomalies)} motion anomaly event(s):")
        print(anomalies.to_string(index=False))
        print()

    # Count anomalies per run
    summary = con.execute(f"""
        WITH magnitude AS (
            SELECT
                ax.run_id,
                ax.timestamp_utc,
                SQRT(
                    ax.imu_ax_mps2 * ax.imu_ax_mps2 +
                    ay.imu_ay_mps2 * ay.imu_ay_mps2 +
                    az.imu_az_mps2 * az.imu_az_mps2
                ) AS accel_mag
            FROM read_parquet('{AX_FILE}') ax
            JOIN read_parquet('{AY_FILE}') ay
                ON ax.run_id = ay.run_id AND ax.seq = ay.seq
            JOIN read_parquet('{AZ_FILE}') az
                ON ax.run_id = az.run_id AND ax.seq = az.seq
        ),
        stats AS (
            SELECT
                run_id,
                accel_mag,
                AVG(accel_mag) OVER (
                    PARTITION BY run_id
                    ORDER BY timestamp_utc
                    ROWS BETWEEN {ROLLING_WINDOW} PRECEDING AND CURRENT ROW
                ) AS mean,
                STDDEV_SAMP(accel_mag) OVER (
                    PARTITION BY run_id
                    ORDER BY timestamp_utc
                    ROWS BETWEEN {ROLLING_WINDOW} PRECEDING AND CURRENT ROW
                ) AS std
            FROM magnitude
        )
        SELECT run_id, COUNT(*) AS anomaly_count
        FROM stats
        WHERE std > 0
        AND ABS((accel_mag - mean) / std) >= {Z_SCORE_THRESHOLD}
        GROUP BY run_id
        ORDER BY run_id
    """).fetchdf()

    if summary.empty:
        print("Anomaly count per run: none")
    else:
        print("Anomaly count per run:")
        print(summary.to_string(index=False))
        # print()

if __name__ == "__main__":
    main()
